from datetime import datetime
from typing import Any

from src.processes.extractors import RubyGemsPackageExtractor
from src.schemas import RubyGemsPackageSchema
from src.services import PackageService, RubyGemsService, VersionService
from src.utils import Attributor


class RubyGemsVersionUpdater:
    def __init__(
        self,
        rubygems_service: RubyGemsService,
        package_service: PackageService,
        version_service: VersionService,
        attributor: Attributor,
    ):
        self.rubygems_service = rubygems_service
        self.package_service = package_service
        self.version_service = version_service
        self.attributor = attributor

    async def update_package_versions(self, package: dict[str, Any]) -> None:
        package_name = package.get("name", "")

        metadata = await self.rubygems_service.fetch_package_metadata(package_name)
        versions = await self.rubygems_service.get_versions(metadata)
        repository_url = self.rubygems_service.get_repo_url(metadata)
        vendor = repository_url.split("/")[-2] if repository_url else None

        count = await self.version_service.count_number_of_versions_by_package(
            "RubyGemsPackage", package_name
        )
        if count < len(versions):
            new_attributed_versions: list[dict[str, Any]] = []

            actual_versions = await self.version_service.read_versions_names_by_package(
                "RubyGemsPackage", package_name
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
                "RubyGemsPackage",
                package_name,
                new_attributed_versions,
            )

            await self.version_service.update_versions_serial_number(
                "RubyGemsPackage", package_name, versions
            )

            for version in created_versions:
                package_schema = RubyGemsPackageSchema(
                    name=package_name,
                    vendor=vendor or "n/a",
                    repository_url=repository_url or "n/a",
                    moment=datetime.now(),
                )
                extractor = RubyGemsPackageExtractor(
                    package=package_schema,
                    package_service=self.package_service,
                    version_service=self.version_service,
                    rubygems_service=self.rubygems_service,
                    attributor=self.attributor,
                )
                await extractor.extract_packages(package_name, version)

        await self.package_service.update_package_moment(
            "RubyGemsPackage", package_name
        )
