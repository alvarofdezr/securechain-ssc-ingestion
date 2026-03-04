from datetime import datetime
from typing import Any

from src.processes.extractors import MavenPackageExtractor
from src.schemas import MavenPackageSchema
from src.services import MavenService, PackageService, VersionService
from src.utils import Attributor


class MavenVersionUpdater:
    def __init__(
        self,
        maven_service: MavenService,
        package_service: PackageService,
        version_service: VersionService,
        attributor: Attributor,
    ):
        self.maven_service = maven_service
        self.package_service = package_service
        self.version_service = version_service
        self.attributor = attributor

    async def update_package_versions(self, package: dict[str, Any]) -> None:
        package_name = package.get("name", "")

        if ":" not in package_name:
            return

        group_id, artifact_id = package_name.split(":", 1)

        metadata = await self.maven_service.fetch_package_metadata(
            group_id, artifact_id
        )
        versions = await self.maven_service.get_versions(metadata)
        repository_url = await self.maven_service.get_repo_url(metadata)
        vendor = repository_url.split("/")[-2] if repository_url else None

        count = await self.version_service.count_number_of_versions_by_package(
            "MavenPackage", package_name
        )
        if count < len(versions):
            new_attributed_versions: list[dict[str, Any]] = []

            actual_versions = await self.version_service.read_versions_names_by_package(
                "MavenPackage", package_name
            )

            for index, version in enumerate(versions):
                if version.get("name", "") not in actual_versions:
                    new_attributed_versions.append(
                        await self.attributor.attribute_vulnerabilities(
                            package_name, version
                        )
                    )
                    del versions[index]

            created_versions = await self.version_service.create_versions(
                "MavenPackage",
                package_name,
                new_attributed_versions,
            )

            await self.version_service.update_versions_serial_number(
                "MavenPackage", package_name, versions
            )

            for version in created_versions:
                if not vendor:
                    vendor = group_id.split(".")[0] if "." in group_id else group_id

                package_schema = MavenPackageSchema(
                    group_id=group_id,
                    artifact_id=artifact_id,
                    name=package_name,
                    vendor=vendor or "n/a",
                    repository_url=repository_url or "n/a",
                    moment=datetime.now(),
                )
                extractor = MavenPackageExtractor(
                    package=package_schema,
                    package_service=self.package_service,
                    version_service=self.version_service,
                    maven_service=self.maven_service,
                    attributor=self.attributor,
                )
                await extractor.extract_packages(package_name, version)

        await self.package_service.update_package_moment("MavenPackage", package_name)
