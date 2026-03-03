from __future__ import annotations

from datetime import datetime
from typing import Any

from src.schemas import GoPackageSchema
from src.services import PackageService, VersionService
from src.services.apis.go_service import GoService
from src.utils import Attributor

from .base import PackageExtractor


class GoPackageExtractor(PackageExtractor):
    """
    Extraction orchestrator for Go module packages.

    Coordinates the full ingestion lifecycle for a single Go module: fetching
    available versions from the proxy, attributing CVE data via the Attributor,
    persisting the package and version nodes through PackageService, and
    recursively resolving transitive dependencies by parsing the go.mod for
    each ingested version.

    Inherits common constraint, parent linkage, and refresh handling from
    PackageExtractor. The go_service dependency is injected rather than
    instantiated internally to allow sharing a single client instance across
    multiple extractor calls within the same Dagster asset run.
    """

    def __init__(
        self,
        package: GoPackageSchema,
        package_service: PackageService,
        version_service: VersionService,
        go_service: GoService,
        attributor: Attributor,
        **kwargs: Any,
    ) -> None:
        """
        Initialise the extractor with all required service dependencies.

        Args:
            package:         Schema instance carrying the module name and
                             pre-populated metadata for the root package.
            package_service: Service for graph-level package CRUD operations.
            version_service: Service for graph-level version CRUD operations.
            go_service:      HTTP client for the Go proxy and index APIs.
            attributor:      Utility that enriches version dicts with CVE
                             attribution data from the vulnerability service.
            **kwargs:        Forwarded to PackageExtractor for constraint,
                             parent_id, parent_version, and refresh fields.
        """
        super().__init__(**kwargs)
        self.package = package
        self.package_service = package_service
        self.version_service = version_service
        self.go_service = go_service
        self.attributor = attributor

    async def run(self) -> None:
        """
        Entry point for the extraction pipeline.

        Triggers the full create-and-link flow for the root package carried
        by this extractor instance, applying any constraints and parent linkage
        provided at construction time.
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
        """
        Process a batch of module requirements and link them to a parent version
        node in the graph.

        For each (module_path, version_constraint) pair, checks whether the
        package already exists in the graph. Known packages are collected and
        bulk-related to the parent in a single service call to minimise
        round-trips. Unknown packages are individually created and linked via
        create_package.

        Args:
            requirement:         Mapping of module paths to version constraint
                                 strings as parsed from a go.mod file.
            parent_id:           Graph node ID of the parent version that
                                 declares these requirements.
            parent_version_name: Version string of the parent, used to establish
                                 the directed dependency edge.
        """
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
        Fetch, attribute, and persist a Go module package with all its versions.

        Retrieves the ordered version list from the proxy, attributes CVE data
        to each version, constructs the GoPackageSchema, and delegates
        persistence to PackageService. For each successfully created version
        the transitive dependency extraction is triggered to build the deeper
        dependency graph.

        Packages with no discoverable versions (e.g. retracted modules or
        private repositories not served by the proxy) are silently skipped
        to avoid creating orphan nodes in the graph.

        Args:
            package_name:        Canonical module path.
            constraints:         Version constraint string from the parent
                                 go.mod, or None for the root package.
            parent_id:           Graph node ID of the declaring parent version,
                                 or None for root-level packages.
            parent_version_name: Version string of the parent, or None.
        """
        versions = await self.go_service.get_versions(package_name)
        if not versions:
            return

        repository_url = self.go_service.get_repo_url(package_name)
        parts = package_name.split("/")
        vendor = parts[0] if parts else "n/a"
        
        latest_version = versions[-1]["name"]
        import_names = await self.go_service.get_import_names(package_name, latest_version)

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

        for created_version in created_versions:
            await self.extract_packages(package_name, created_version)

        await self.version_service.update_versions_serial_number(
            "GoPackage", package_name, versions
        )
        await self.package_service.update_package_moment("GoPackage", package_name)

    async def extract_packages(
        self, parent_package_name: str, version: dict[str, Any]
    ) -> None:
        """
        Resolve and ingest the direct dependencies declared in a specific
        version's go.mod file.

        Fetches the go.mod for the given version from the proxy, parses its
        require directives, and delegates to generate_packages to create or
        link the dependency nodes. This is the recursive step that builds the
        transitive dependency graph.

        Args:
            parent_package_name: Module path of the package whose go.mod is
                                 being resolved.
            version:             Version descriptor dict containing at least
                                 'name' (version string) and 'id' (graph node ID).
        """
        requirements = await self.go_service.get_package_requirements(
            parent_package_name, version.get("name", "")
        )
        if requirements:
            await self.generate_packages(
                requirements,
                version.get("id", ""),
                parent_package_name,
            )
