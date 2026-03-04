from datetime import datetime

from src.schemas import NPMPackageSchema
from src.services import NPMService, PackageService, VersionService
from src.utils import Attributor

from .base import PackageExtractor


class NPMPackageExtractor(PackageExtractor):
    def __init__(
        self,
        package: NPMPackageSchema,
        package_service: PackageService,
        version_service: VersionService,
        npm_service: NPMService,
        attributor: Attributor,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.package = package
        self.package_service = package_service
        self.version_service = version_service
        self.npm_service = npm_service
        self.attributor = attributor

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
        known_packages = []
        for package_name, constraints in requirement.items():
            package_name = package_name.lower()
            package = await self.package_service.read_package_by_name(
                "NPMPackage", package_name
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
        await self.package_service.relate_packages("NPMPackage", known_packages)

    async def create_package(
        self,
        package_name: str,
        constraints: str | None = None,
        parent_id: str | None = None,
        parent_version_name: str | None = None,
    ) -> None:
        metadata = await self.npm_service.fetch_package_metadata(package_name)
        versions, requirements = await self.npm_service.get_versions_and_requirements(
            metadata
        )
        repository_url = self.npm_service.get_repo_url(metadata)
        vendor = repository_url.split("/")[-2] if repository_url else None

        if not versions:
            return

        attributed_versions = [
            await self.attributor.attribute_vulnerabilities(package_name, version)
            for version in versions
        ]

        if not vendor:
            vendor = package_name.split("/")[0] if "@" in package_name else None

        import_names = []
        if versions:
            latest_version = versions[-1].get("name")
            if latest_version:
                import_names = await self.npm_service.extract_import_names(
                    package_name, latest_version
                )

        pkg = NPMPackageSchema(
            name=package_name,
            vendor=vendor or "n/a",
            repository_url=repository_url or "n/a",
            moment=datetime.now(),
            import_names=import_names,
        )

        created_versions = await self.package_service.create_package_and_versions(
            "NPMPackage",
            pkg.to_dict(),
            attributed_versions,
            constraints,
            parent_id,
            parent_version_name,
        )

        for created_version, requirement in zip(
            created_versions, requirements, strict=False
        ):
            await self.generate_packages(
                requirement, created_version.get("id", ""), package_name
            )
