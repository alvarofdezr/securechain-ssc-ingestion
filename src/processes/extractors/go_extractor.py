from __future__ import annotations

import re
from asyncio import Semaphore
from datetime import datetime
from typing import Any

from src.logger import logger
from src.schemas import GoPackageSchema
from src.services import PackageService, VersionService
from src.services.apis.go_service import GoService
from src.utils import Attributor

from .base import PackageExtractor

_MAX_DEPTH = 2
_GO_SEMAPHORE = Semaphore(10)
_IN_PROGRESS: set[str] = set()


class GoPackageExtractor(PackageExtractor):
    """Extracts, attributes, and persists Go modules with their dependency graph.
    
    Orchestrates the ingestion pipeline for Go packages, including version discovery,
    vulnerability attribution, and recursive dependency extraction. Implements depth-based
    recursion limiting and anti-circularity safeguards to prevent infinite traversal
    of circular dependency graphs.
    
    Uses a module-level semaphore to throttle concurrent requests to the Go proxy
    API (max 10 concurrent). A module-level set tracks packages currently being
    processed to prevent re-entrant calls regardless of recursion depth.
    
    Attributes:
        package: GoPackageSchema describing the package being extracted.
        package_service: Service for package CRUD and relationship operations.
        version_service: Service for version metadata and serial number management.
        go_service: Go proxy API client for fetching versions and dependencies.
        attributor: Vulnerability attribution service for versions.
        _depth: Current recursion depth (0 for root, incremented for dependencies).
    """

    def __init__(
        self,
        package: GoPackageSchema,
        package_service: PackageService,
        version_service: VersionService,
        go_service: GoService,
        attributor: Attributor,
        _depth: int = 0,
        **kwargs: Any,
    ) -> None:
        """Initializes a Go package extractor with required service dependencies.
        
        Args:
            package: The Go package schema to extract.
            package_service: Package data access service.
            version_service: Version data access service.
            go_service: Go proxy API client.
            attributor: Vulnerability attribution engine.
            _depth: Current recursion depth (default: 0 for root extraction).
            **kwargs: Additional arguments passed to parent PackageExtractor.
        """
        super().__init__(**kwargs)
        self.package = package
        self.package_service = package_service
        self.version_service = version_service
        self.go_service = go_service
        self.attributor = attributor
        self._depth = _depth

    async def run(self) -> None:
        """Initiates package extraction as the primary entry point.
        
        Delegates to create_package with root-level parameters.
        """
        await self.create_package(
            self.package.name,
            self.constraints,
            self.parent_id,
            self.parent_version_name,
        )

    async def generate_packages(
        self,
        requirement: dict[str, str],
        parent_id: str,
        parent_version_name: str | None = None,
    ) -> None:
        """Processes a batch of module requirements and links them to a parent version.
        
        Classifies packages into known (already persisted) and unknown (new) categories.
        Known packages are bulk-related to the parent; unknown packages are created
        individually only when recursion depth is below _MAX_DEPTH to prevent
        unbounded traversal. At or beyond maximum depth, unknown packages are skipped.
        
        Args:
            requirement: Dictionary mapping package names to version constraints.
            parent_id: The parent version element ID to establish relationships.
            parent_version_name: Semantic version identifier of the parent module
                (used for transitive dependency tracking).
        
        Raises:
            Exception: Propagates exceptions from package service operations.
        """
        if self._depth >= _MAX_DEPTH:
            known_packages: list[dict[str, Any]] = []
            for package_name, constraints in requirement.items():
                if not package_name or not package_name.strip():
                    logger.warning("Go - Skipping empty package name")
                    continue
                package = await self.package_service.read_package_by_name(
                    "GoPackage", package_name
                )
                if package:
                    package["parent_id"] = parent_id
                    package["parent_version_name"] = parent_version_name
                    package["constraints"] = constraints
                    known_packages.append(package)

            if known_packages:
                await self.package_service.relate_packages("GoPackage", known_packages)
            return

        known_packages = []
        for package_name, constraints in requirement.items():
            if not package_name or not package_name.strip():
                logger.warning("Go - Skipping empty package name")
                continue

            package = await self.package_service.read_package_by_name(
                "GoPackage", package_name
            )
            if package:
                package["parent_id"] = parent_id
                package["parent_version_name"] = parent_version_name
                package["constraints"] = constraints
                known_packages.append(package)
            else:
                await self.create_package(
                    package_name, constraints, parent_id, parent_version_name
                )

        await self.package_service.relate_packages("GoPackage", known_packages)

    async def create_package(
        self,
        package_name: str,
        constraints: str | None = None,
        parent_id: str | None = None,
        parent_version_name: str | None = None,
    ) -> None:
        """Fetches, attributes, and persists a Go module with all its versions.
        
        Orchestrates the complete ingestion workflow for a single package:
        1. Anti-circularity check: skip if already in progress.
        2. Normalize major version suffix (e.g., github.com/user/pkg/v2 -> github.com/user/pkg).
        3. Fetch versions from Go proxy with semaphore-controlled concurrency.
        4. Extract repository URL and vendor information.
        5. Fetch import names for the latest version.
        6. Attribute all versions with vulnerability data.
        7. Persist package and versions to graph with relationships.
        8. If below recursion depth, recursively extract dependencies from latest version.
        9. Update version serial numbers for ordering consistency.
        
        Packages with no discoverable versions are silently skipped. A module-level
        _IN_PROGRESS set prevents re-entrant calls for the same package, providing
        circular dependency protection independent of depth-based recursion limiting.
        
        Args:
            package_name: The Go module name (e.g., github.com/user/repo or github.com/user/repo/v2).
            constraints: Optional version constraint specification for the parent relationship.
            parent_id: Optional parent version element ID for establishing relationships.
            parent_version_name: Optional semantic version identifier of the parent.
        
        Raises:
            Exception: Logged and suppressed; errors are reported but do not halt processing.
        """
        if package_name in _IN_PROGRESS:
            logger.debug(
                f"Go - [{package_name}] Already in progress, skipping to prevent cycle."
            )
            return

        _IN_PROGRESS.add(package_name)

        api_package_name = package_name
        if re.search(r'/v[2-9][0-9]*$', package_name):
            api_package_name = package_name.rsplit('/v', 1)[0]

        try:
            async with _GO_SEMAPHORE:
                versions = await self.go_service.get_versions(api_package_name)

            if not versions:
                return

            repository_url = self.go_service.get_repo_url(api_package_name)
            parts = package_name.split("/")
            vendor = parts[0] if parts else "n/a"
            latest_version = versions[-1]["name"]

            async with _GO_SEMAPHORE:
                import_names = await self.go_service.get_import_names(
                    api_package_name, latest_version
                )

            attributed_versions = [
                await self.attributor.attribute_vulnerabilities(package_name, version)
                for version in versions
            ]

            pkg = GoPackageSchema(
                name=package_name,
                vendor=vendor,
                repository_url=repository_url,
                moment=datetime.now(),
                import_names=import_names,
            )

            created_versions = await self.package_service.create_package_and_versions(
                "GoPackage",
                pkg.to_dict(),
                attributed_versions,
                constraints,
                parent_id,
                parent_version_name,
            )

            if self._depth < _MAX_DEPTH and created_versions:
                latest_created = created_versions[-1]
                await self.extract_packages(package_name, latest_created)

            await self.version_service.update_versions_serial_number(
                "GoPackage", package_name, attributed_versions
            )
            await self.package_service.update_package_moment("GoPackage", package_name)

        except Exception as e:
            logger.error(f"Go - Error in create_package ({package_name}): {e}")
        finally:
            _IN_PROGRESS.discard(package_name)

    async def extract_packages(
        self, parent_package_name: str, version: dict[str, Any]
    ) -> None:
        """Resolves and ingests the direct dependencies declared in a version's go.mod file.
        
        Fetches the dependency list for a specific module version from the Go proxy,
        then creates a child extractor with incremented recursion depth to process
        those dependencies. Recursion depth enforcement ensures termination of deep
        or circular dependency graphs within the _MAX_DEPTH limit.
        
        Args:
            parent_package_name: The module name (before version normalization).
            version: Dictionary containing 'name' (semantic version) and 'id' (element ID).
        
        Raises:
            Exception: Propagates exceptions from Go proxy API or extractor operations.
        """
        async with _GO_SEMAPHORE:
            requirements = await self.go_service.get_package_requirements(
                parent_package_name, version.get("name", "")
            )

        if not requirements:
            return

        child_extractor = GoPackageExtractor(
            package=self.package,
            package_service=self.package_service,
            version_service=self.version_service,
            go_service=self.go_service,
            attributor=self.attributor,
            _depth=self._depth + 1,
        )

        await child_extractor.generate_packages(
            requirements,
            version.get("id", ""),
            parent_package_name,
        )
