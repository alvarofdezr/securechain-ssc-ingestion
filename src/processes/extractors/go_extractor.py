from __future__ import annotations

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
    """
    Extraction orchestrator for Go module packages.

    Coordinates the full ingestion lifecycle for a single Go module: fetching
    available versions from the proxy, attributing CVE data via the Attributor,
    persisting the package and version nodes through PackageService, and
    recursively resolving transitive dependencies by parsing the go.mod for
    each ingested version.

    Recursion is capped at _MAX_DEPTH to prevent infinite traversal of Go's
    dense dependency graph. A module-level semaphore throttles concurrent
    proxy requests across all extractor instances in a run.
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
        super().__init__(**kwargs)
        self.package = package
        self.package_service = package_service
        self.version_service = version_service
        self.go_service = go_service
        self.attributor = attributor
        self._depth = _depth

    async def run(self) -> None:
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
        """
        Process a batch of module requirements and link them to a parent version.

        Known packages are bulk-related; unknown packages are created individually
        only when the current depth is below _MAX_DEPTH to prevent unbounded
        recursive traversal.
        """
        if self._depth >= _MAX_DEPTH:
            known_packages: list[dict[str, Any]] = []
            for package_name, constraints in requirement.items():
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
        """
        Fetch, attribute, and persist a Go module with all its versions.

        Uses the module-level semaphore to throttle proxy requests. Packages
        with no discoverable versions are silently skipped. A module-level
        _IN_PROGRESS set prevents re-entrant calls for the same package,
        guarding against circular dependency graphs independently of the
        depth cap.
        """
        if package_name in _IN_PROGRESS:
            logger.debug(
                f"Go - [{package_name}] Already in progress, skipping to prevent cycle."
            )
            return
        _IN_PROGRESS.add(package_name)

        try:
            async with _GO_SEMAPHORE:
                versions = await self.go_service.get_versions(package_name)

            if not versions:
                return

            repository_url = self.go_service.get_repo_url(package_name)
            parts = package_name.split("/")
            vendor = parts[0] if parts else "n/a"

            latest_version = versions[-1]["name"]

            async with _GO_SEMAPHORE:
                import_names = await self.go_service.get_import_names(
                    package_name, latest_version
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
                "GoPackage", package_name, versions
            )
            await self.package_service.update_package_moment("GoPackage", package_name)
        finally:
            _IN_PROGRESS.discard(package_name)

    async def extract_packages(
        self, parent_package_name: str, version: dict[str, Any]
    ) -> None:
        """
        Resolve and ingest the direct dependencies declared in a version's go.mod.

        Creates a child extractor with depth incremented by one so that the
        recursion cap is enforced at each level of the dependency tree.
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
