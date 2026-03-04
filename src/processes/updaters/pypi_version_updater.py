from datetime import datetime
from typing import Any

from src.processes.extractors import PyPIPackageExtractor
from src.schemas import PyPIPackageSchema
from src.services import PackageService, PyPIService, VersionService
from src.utils import Attributor


class PyPIVersionUpdater:
    def __init__(
        self,
        pypi_service: PyPIService,
        package_service: PackageService,
        version_service: VersionService,
        attributor: Attributor,
    ):
        self.pypi_service = pypi_service
        self.package_service = package_service
        self.version_service = version_service
        self.attributor = attributor

    async def update_package_versions(self, package: dict[str, Any]) -> None:
        package_name = package.get("name", "")

        metadata = await self.pypi_service.fetch_package_metadata(package_name)
        versions = await self.pypi_service.get_versions(metadata)
        repository_url = self.pypi_service.get_repo_url(metadata)
        vendor = repository_url.split("/")[-2] if repository_url else None

        count = await self.version_service.count_number_of_versions_by_package(
            "PyPIPackage", package_name
        )
        if count < len(versions):
            new_attributed_versions: list[dict[str, Any]] = []

            actual_versions = await self.version_service.read_versions_names_by_package(
                "PyPIPackage", package_name
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
                "PyPIPackage",
                package_name,
                new_attributed_versions,
            )

            await self.version_service.update_versions_serial_number(
                "PyPIPackage", package_name, versions
            )

            for version in created_versions:
                package_schema = PyPIPackageSchema(
                    name=package_name,
                    vendor=vendor or "n/a",
                    repository_url=repository_url or "n/a",
                    moment=datetime.now(),
                )
                extractor = PyPIPackageExtractor(
                    package=package_schema,
                    package_service=self.package_service,
                    version_service=self.version_service,
                    pypi_service=self.pypi_service,
                    attributor=self.attributor,
                )
                await extractor.extract_packages(package_name, version)

        await self.package_service.update_package_moment("PyPIPackage", package_name)
