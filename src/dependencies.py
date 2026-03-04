from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from aiohttp import ClientTimeout

from src.cache import CacheManager
from src.database import DatabaseManager
from src.session import SessionManager
from src.settings import settings
from src.utils import (
    Attributor,
    Orderer,
    PyPIConstraintsParser,
    RedisQueue,
    RepoNormalizer,
)

if TYPE_CHECKING:
    from src.services import (
        CargoService,
        GoService,
        MavenService,
        NPMService,
        NuGetService,
        PackageService,
        PyPIService,
        RubyGemsService,
        VersionService,
        VulnerabilityService,
    )


class ServiceContainer:
    instance: ServiceContainer | None = None
    db_manager: DatabaseManager | None = None
    session_manager: SessionManager | None = None
    cargo_service: Any = None
    maven_service: Any = None
    npm_service: Any = None
    nuget_service: Any = None
    pypi_service: Any = None
    rubygems_service: Any = None
    go_service: Any = None
    package_service: Any = None
    version_service: Any = None
    vulnerability_service: Any = None
    redis_queue: RedisQueue | None = None
    attributor: Attributor | None = None
    repo_normalizer: RepoNormalizer | None = None
    pypi_constraints_parser: PyPIConstraintsParser | None = None
    cache_managers: ClassVar[dict[str, CacheManager]] = {}
    orderers: ClassVar[dict[str, Orderer]] = {}
    default_timeout: ClientTimeout | None = None
    medium_timeout: ClientTimeout | None = None
    long_timeout: ClientTimeout | None = None

    def __new__(cls) -> ServiceContainer:
        if cls.instance is None:
            cls.instance = super().__new__(cls)
        return cls.instance

    def get_db(self) -> DatabaseManager:
        if self.db_manager is None:
            self.db_manager = DatabaseManager()
        return self.db_manager

    def get_session_manager(self) -> SessionManager:
        if self.session_manager is None:
            self.session_manager = SessionManager()
        return self.session_manager

    def get_cache_manager(self, manager_name: str) -> CacheManager:
        if manager_name not in self.cache_managers:
            self.cache_managers[manager_name] = CacheManager(manager_name)
        return self.cache_managers[manager_name]

    def get_orderer(self, node_type: str) -> Orderer:
        if node_type not in self.orderers:
            self.orderers[node_type] = Orderer(node_type)
        return self.orderers[node_type]

    def get_repo_normalizer(self) -> RepoNormalizer:
        if self.repo_normalizer is None:
            self.repo_normalizer = RepoNormalizer()
        return self.repo_normalizer

    def get_pypi_constraints_parser(self) -> PyPIConstraintsParser:
        if self.pypi_constraints_parser is None:
            self.pypi_constraints_parser = PyPIConstraintsParser()
        return self.pypi_constraints_parser

    def get_attributor(self) -> Attributor:
        if self.attributor is None:
            self.attributor = Attributor(self.get_vulnerability_service())
        return self.attributor

    def get_cargo_service(self) -> CargoService:
        if self.cargo_service is None:
            from src.services import CargoService

            self.cargo_service = CargoService()
        return self.cargo_service

    def get_maven_service(self) -> MavenService:
        if self.maven_service is None:
            from src.services import MavenService

            self.maven_service = MavenService()
        return self.maven_service

    def get_npm_service(self) -> NPMService:
        if self.npm_service is None:
            from src.services import NPMService

            self.npm_service = NPMService()
        return self.npm_service

    def get_nuget_service(self) -> NuGetService:
        if self.nuget_service is None:
            from src.services import NuGetService

            self.nuget_service = NuGetService()
        return self.nuget_service

    def get_pypi_service(self) -> PyPIService:
        if self.pypi_service is None:
            from src.services import PyPIService

            self.pypi_service = PyPIService()
        return self.pypi_service

    def get_rubygems_service(self) -> RubyGemsService:
        if self.rubygems_service is None:
            from src.services import RubyGemsService

            self.rubygems_service = RubyGemsService()
        return self.rubygems_service

    def get_go_service(self) -> GoService:
        if self.go_service is None:
            from src.services.apis.go_service import GoService

            self.go_service = GoService()
        return self.go_service

    def get_package_service(self) -> PackageService:
        if self.package_service is None:
            from src.services import PackageService

            self.package_service = PackageService(self.get_db())
        return self.package_service

    def get_version_service(self) -> VersionService:
        if self.version_service is None:
            from src.services import VersionService

            self.version_service = VersionService(self.get_db())
        return self.version_service

    def get_vulnerability_service(self) -> VulnerabilityService:
        if self.vulnerability_service is None:
            from src.services import VulnerabilityService

            self.vulnerability_service = VulnerabilityService(self.get_db())
        return self.vulnerability_service

    def get_redis_queue(self) -> RedisQueue:
        if self.redis_queue is None:
            self.redis_queue = RedisQueue.from_env()
        return self.redis_queue

    def get_default_timeout(self) -> ClientTimeout:
        if self.default_timeout is None:
            self.default_timeout = ClientTimeout(total=settings.HTTP_TIMEOUT_DEFAULT)
        return self.default_timeout

    def get_medium_timeout(self) -> ClientTimeout:
        if self.medium_timeout is None:
            self.medium_timeout = ClientTimeout(total=settings.HTTP_TIMEOUT_MEDIUM)
        return self.medium_timeout

    def get_long_timeout(self) -> ClientTimeout:
        if self.long_timeout is None:
            self.long_timeout = ClientTimeout(total=settings.HTTP_TIMEOUT_LONG)
        return self.long_timeout


def get_db() -> DatabaseManager:
    return ServiceContainer().get_db()


def get_cargo_service() -> CargoService:
    return ServiceContainer().get_cargo_service()


def get_maven_service() -> MavenService:
    return ServiceContainer().get_maven_service()


def get_session_manager() -> SessionManager:
    return ServiceContainer().get_session_manager()


def get_cache_manager(manager_name: str) -> CacheManager:
    return ServiceContainer().get_cache_manager(manager_name)


def get_orderer(node_type: str) -> Orderer:
    return ServiceContainer().get_orderer(node_type)


def get_repo_normalizer() -> RepoNormalizer:
    return ServiceContainer().get_repo_normalizer()


def get_pypi_constraints_parser() -> PyPIConstraintsParser:
    return ServiceContainer().get_pypi_constraints_parser()


def get_attributor() -> Attributor:
    return ServiceContainer().get_attributor()


def get_npm_service() -> NPMService:
    return ServiceContainer().get_npm_service()


def get_nuget_service() -> NuGetService:
    return ServiceContainer().get_nuget_service()


def get_pypi_service() -> PyPIService:
    return ServiceContainer().get_pypi_service()


def get_rubygems_service() -> RubyGemsService:
    return ServiceContainer().get_rubygems_service()


def get_go_service() -> GoService:
    return ServiceContainer().get_go_service()


def get_package_service() -> PackageService:
    return ServiceContainer().get_package_service()


def get_version_service() -> VersionService:
    return ServiceContainer().get_version_service()


def get_vulnerability_service() -> VulnerabilityService:
    return ServiceContainer().get_vulnerability_service()


def get_redis_queue() -> RedisQueue:
    return ServiceContainer().get_redis_queue()


def get_default_timeout() -> ClientTimeout:
    return ServiceContainer().get_default_timeout()


def get_medium_timeout() -> ClientTimeout:
    return ServiceContainer().get_medium_timeout()


def get_long_timeout() -> ClientTimeout:
    return ServiceContainer().get_long_timeout()
