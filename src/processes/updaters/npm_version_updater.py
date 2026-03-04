from datetime import datetime
from typing import Any

from src.processes.extractors import NPMPackageExtractor
from src.schemas import NPMPackageSchema
from src.services import NPMService, PackageService, VersionService
from src.utils import Attributor


class NPMVersionUpdater:
    def __init__(
        self,
        npm_service: NPMService,
        package_service: PackageService,
        version_service: VersionService,
        attributor: Attributor,
    ):
        self.npm_service = npm_service
        self.package_service = package_service
        self.version_service = version_service
        self.attributor = attributor

    async def update_package_versions(self, package: dict[str, Any]) -> None:
        package_name = package.get("name", "")

        metadata = await self.npm_service.fetch_package_metadata(package_name)
        versions, requirements = await self.npm_service.get_versions_and_requirements(
            metadata
        )
        repository_url = self.npm_service.get_repo_url(metadata)
        vendor = repository_url.split("/")[-2] if repository_url else None

        count = await self.version_service.count_number_of_versions_by_package(
            "NPMPackage", package_name
        )
        if count < len(versions):
            new_attributed_versions: list[dict[str, Any]] = []
            new_requirements = []
            actual_versions = await self.version_service.read_versions_names_by_package(
                "NPMPackage", package_name
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
                "NPMPackage",
                package_name,
                new_attributed_versions,
            )

            await self.version_service.update_versions_serial_number(
                "NPMPackage", package_name, versions
            )

            if not vendor:
                vendor = package_name.split("/")[0] if "@" in package_name else "n/a"

            package_schema = NPMPackageSchema(
                name=package_name,
                vendor=vendor,
                repository_url=repository_url or "n/a",
                moment=datetime.now(),
            )

            for version, requirement in zip(
                created_versions, new_requirements, strict=False
            ):
                extractor = NPMPackageExtractor(
                    package=package_schema,
                    package_service=self.package_service,
                    version_service=self.version_service,
                    npm_service=self.npm_service,
                    attributor=self.attributor,
                )
                if requirement:
                    await extractor.generate_packages(
                        requirement, version.get("id", ""), package_name
                    )

        await self.package_service.update_package_moment("NPMPackage", package_name)
