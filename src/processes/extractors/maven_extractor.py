from datetime import datetime
from typing import Any

from src.schemas import MavenPackageSchema
from src.services import MavenService, PackageService, VersionService
from src.utils import Attributor

from .base import PackageExtractor


class MavenPackageExtractor(PackageExtractor):
    def __init__(
        self,
        package: MavenPackageSchema,
        package_service: PackageService,
        version_service: VersionService,
        maven_service: MavenService,
        attributor: Attributor,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.package = package
        self.package_service = package_service
        self.version_service = version_service
        self.maven_service = maven_service
        self.attributor = attributor

    async def run(self) -> None:
        await self.create_package(
            self.package.group_id,
            self.package.artifact_id,
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
        known_packages = []
        for package_name, constraints in requirement.items():
            package_name = package_name.lower()
            if ":" not in package_name:
                continue

            group_id, artifact_id = package_name.split(":")
            package = await self.package_service.read_package_by_name(
                "MavenPackage", package_name
            )
            if package:
                package["parent_id"] = parent_id
                package["parent_version_name"] = parent_version_name
                package["constraints"] = constraints
                known_packages.append(package)
            else:
                await self.create_package(
                    group_id, artifact_id, constraints, parent_id, parent_version_name
                )

        await self.package_service.relate_packages("MavenPackage", known_packages)

    async def create_package(
        self,
        group_id: str,
        artifact_id: str,
        constraints: str | None = None,
        parent_id: str | None = None,
        parent_version_name: str | None = None,
    ) -> None:
        metadata = await self.maven_service.fetch_package_metadata(
            group_id, artifact_id
        )
        versions = await self.maven_service.get_versions(metadata)
        repository_url = await self.maven_service.get_repo_url(metadata)
        vendor = repository_url.split("/")[-2] if repository_url else None

        if not versions:
            return

        package_name = f"{group_id}:{artifact_id}"
        attributed_versions = [
            await self.attributor.attribute_vulnerabilities(package_name, version)
            for version in versions
        ]

        if not vendor:
            vendor = group_id.split(".")[0] if "." in group_id else group_id

        import_names = []
        if versions:
            latest_version = versions[-1].get("name")
            if latest_version:
                import_names = await self.maven_service.extract_import_names(
                    group_id, artifact_id, latest_version
                )

        pkg = MavenPackageSchema(
            group_id=group_id,
            artifact_id=artifact_id,
            name=package_name,
            vendor=vendor or "n/a",
            repository_url=repository_url or "n/a",
            moment=datetime.now(),
            import_names=import_names,
        )

        created_versions = await self.package_service.create_package_and_versions(
            "MavenPackage",
            pkg.to_dict(),
            attributed_versions,
            constraints,
            parent_id,
            parent_version_name,
        )

        for created_version in created_versions:
            await self.extract_packages(package_name, created_version)

        await self.version_service.update_versions_serial_number(
            "MavenPackage", package_name, versions
        )
        await self.package_service.update_package_moment("MavenPackage", package_name)

    async def extract_packages(
        self, parent_package_name: str, version: dict[str, Any]
    ) -> None:
        if ":" not in parent_package_name:
            return

        group_id, artifact_id = parent_package_name.split(":", 1)
        metadata = await self.maven_service.fetch_package_version_metadata(
            group_id, artifact_id, version.get("name", "")
        )
        requirement = self.maven_service.get_package_requirements(metadata)
        if requirement:
            await self.generate_packages(
                requirement, version.get("id", ""), parent_package_name
            )
