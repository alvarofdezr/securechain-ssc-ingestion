from __future__ import annotations

from datetime import datetime
from typing import Any

from src.processes.extractors import GoPackageExtractor
from src.schemas import GoPackageSchema
from src.services import PackageService, VersionService
from src.services.apis.go_service import GoService
from src.utils import Attributor


class GoVersionUpdater:
    """
    Incremental version updater for Go module packages already present in the
    graph database.

    Compares the version list currently stored in the graph against the list
    available from the Go proxy and ingests only the delta: new versions are
    attributed with CVE data, persisted, and their transitive dependencies
    resolved. Existing versions are not re-processed, keeping the update
    operation efficient even for packages with a large version history.
    """

    def __init__(
        self,
        go_service: GoService,
        package_service: PackageService,
        version_service: VersionService,
        attributor: Attributor,
    ) -> None:
        """
        Initialise the updater with all required service dependencies.

        Args:
            go_service:      HTTP client for the Go proxy API.
            package_service: Service for graph-level package operations.
            version_service: Service for graph-level version operations.
            attributor:      CVE attribution utility for version enrichment.
        """
        self.go_service = go_service
        self.package_service = package_service
        self.version_service = version_service
        self.attributor = attributor

    async def update_package_versions(self, package: dict[str, Any]) -> None:
        """
        Synchronise the version list for a single Go module package with the
        upstream proxy.

        Fetches the current version count from the graph and the full version
        list from the proxy. If the proxy reports more versions than are stored
        in the graph, the new versions are identified, attributed, and persisted.
        The serial number sequence for the entire package is then updated to
        reflect the insertion of new entries.

        The package's moment field is always refreshed at the end of the call,
        regardless of whether new versions were found, to accurately reflect
        the time of the last update check.

        Note on iteration safety: new and existing versions are separated using
        list comprehensions before any mutation occurs. Modifying a list while
        iterating over it with enumerate causes index drift and silently skips
        elements, which is the bug this implementation explicitly avoids.

        Args:
            package: Package dict as returned by PackageService, containing at
                     minimum a 'name' key with the canonical module path.
        """
        package_name: str = package.get("name", "")

        versions = await self.go_service.get_versions(package_name)
        repository_url = self.go_service.get_repo_url(package_name)
        parts = package_name.split("/")
        vendor = parts[0] if parts else "n/a"

        stored_count = await self.version_service.count_number_of_versions_by_package(
            "GoPackage", package_name
        )

        if stored_count < len(versions):
            actual_version_names = (
                await self.version_service.read_versions_names_by_package(
                    "GoPackage", package_name
                )
            )

            # Separate new versions from already-stored ones using list
            # comprehensions to avoid mutating the list during iteration,
            # which would cause index drift and silently skip elements.
            new_versions = [
                v for v in versions if v.get("name", "") not in actual_version_names
            ]
            existing_versions = [
                v for v in versions if v.get("name", "") in actual_version_names
            ]

            new_attributed_versions = [
                await self.attributor.attribute_vulnerabilities(package_name, v)
                for v in new_versions
            ]

            created_versions = await self.version_service.create_versions(
                "GoPackage",
                package_name,
                new_attributed_versions,
            )

            await self.version_service.update_versions_serial_number(
                "GoPackage", package_name, existing_versions
            )

            for version in created_versions:
                package_schema = GoPackageSchema(
                    name=package_name,
                    vendor=vendor,
                    repository_url=repository_url,
                    moment=datetime.now(),
                )
                extractor = GoPackageExtractor(
                    package=package_schema,
                    package_service=self.package_service,
                    version_service=self.version_service,
                    go_service=self.go_service,
                    attributor=self.attributor,
                )
                await extractor.extract_packages(package_name, version)

        await self.package_service.update_package_moment("GoPackage", package_name)
