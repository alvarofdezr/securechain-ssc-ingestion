from datetime import datetime
from typing import Any

from src.processes.extractors import NuGetPackageExtractor
from src.schemas import NuGetPackageSchema
from src.services import NuGetService, PackageService, VersionService
from src.utils import Attributor


class NuGetVersionUpdater:
    def __init__(
        self,
        nuget_service: NuGetService,
        package_service: PackageService,
        version_service: VersionService,
        attributor: Attributor,
    ):
        self.nuget_service = nuget_service
        self.package_service = package_service
        self.version_service = version_service
        self.attributor = attributor

    async def update_package_versions(self, package: dict[str, Any]) -> None:
        package_name = package.get("name", "")

        metadata = await self.nuget_service.fetch_package_metadata(package_name)
        versions, requirements = await self.nuget_service.get_versions_and_requirements(
            metadata
        )
        repository_url = await self.nuget_service.get_repo_url(metadata)
        vendor = repository_url.split("/")[-2] if repository_url else None

        count = await self.version_service.count_number_of_versions_by_package(
            "NuGetPackage", package_name
        )
        if count < len(versions):
            new_attributed_versions: list[dict[str, Any]] = []
            new_requirements = []
            actual_versions = await self.version_service.read_versions_names_by_package(
                "NuGetPackage", package_name
            )

            for index, (version, requirement) in enumerate(
                zip(versions, requirements, strict=False)
            ):
                if version.get("name", "") not in actual_versions:
                    new_attributed_versions.append(
                        await self.attributor.attribute_vulnerabilities(
                            package_name, version
                        )
                    )
                    del versions[index]
                    new_requirements.append(requirement)

            created_versions = await self.version_service.create_versions(
                "NuGetPackage",
                package_name,
                new_attributed_versions,
            )

            await self.version_service.update_versions_serial_number(
                "NuGetPackage", package_name, versions
            )

            package_schema = NuGetPackageSchema(
                name=package_name,
                vendor=vendor or "n/a",
                repository_url=repository_url or "n/a",
                moment=datetime.now(),
            )

            for version, requirement in zip(
                created_versions, new_requirements, strict=False
            ):
                extractor = NuGetPackageExtractor(
                    package=package_schema,
                    package_service=self.package_service,
                    version_service=self.version_service,
                    nuget_service=self.nuget_service,
                    attributor=self.attributor,
                )
                if requirement:
                    await extractor.generate_packages(
                        requirement, version.get("id", ""), package_name
                    )

        await self.package_service.update_package_moment("NuGetPackage", package_name)
