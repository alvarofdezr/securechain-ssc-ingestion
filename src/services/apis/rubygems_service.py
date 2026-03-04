from asyncio import sleep, to_thread
from io import BytesIO
from json import JSONDecodeError
from tarfile import TarError
from tarfile import open as open_tarfile
from typing import Any

from aiohttp import ClientConnectorError, ContentTypeError

from src.dependencies import (
    get_cache_manager,
    get_default_timeout,
    get_long_timeout,
    get_orderer,
    get_repo_normalizer,
    get_session_manager,
)
from src.logger import logger


class RubyGemsService:
    def __init__(self):
        self.cache = get_cache_manager("rubygems")
        self.BASE_V1_URL = "https://rubygems.org/api/v1/versions"
        self.BASE_V2_URL = "https://rubygems.org/api/v2/rubygems"
        self.DOWNLOAD_URL = "https://rubygems.org/downloads"
        self.GEMS_NAMES_URL = "https://index.rubygems.org/names"
        self.orderer = get_orderer("RubyGemsPackage")
        self.repo_normalizer = get_repo_normalizer()

    async def fetch_all_package_names(self) -> list[str]:
        cached = await self.cache.get_cache("all_rubygems_packages")
        if cached:
            return cached

        session_manager = get_session_manager()
        session = await session_manager.get_session()

        try:
            async with session.get(
                self.GEMS_NAMES_URL, timeout=get_long_timeout()
            ) as resp:
                if resp.status != 200:
                    logger.error(
                        f"RubyGems - Failed to fetch gem list: status {resp.status}"
                    )
                    return []

                text = await resp.text()
                gem_names = [
                    line.strip() for line in text.strip().split("\n") if line.strip()
                ]

                logger.info(f"RubyGems - Fetched {len(gem_names)} gems")
                await self.cache.set_cache("all_rubygems_packages", gem_names, ttl=3600)
                return gem_names

        except Exception as e:
            logger.error(f"RubyGems - Fatal error in fetch_all_package_names: {e}")
            return []

    async def fetch_package_metadata(self, package_name: str) -> dict[str, Any]:
        cached = await self.cache.get_cache(package_name)
        if cached:
            return cached

        url = f"{self.BASE_V1_URL}/{package_name}.json"
        session_manager = get_session_manager()
        session = await session_manager.get_session()

        for _ in range(3):
            try:
                async with session.get(url) as resp:
                    response = await resp.json()
                    await self.cache.set_cache(package_name, response, ttl=600)
                    return response
            except (ClientConnectorError, TimeoutError):
                await sleep(5)
            except (JSONDecodeError, ContentTypeError):
                return {}
        return {}

    async def fetch_package_version_metadata(
        self, package_name: str, version_name: str
    ) -> dict[str, Any]:
        cache_key = f"{package_name}:{version_name}"
        cached = await self.cache.get_cache(cache_key)
        if cached:
            return cached

        url = f"{self.BASE_V2_URL}/{package_name}/versions/{version_name}.json"
        session_manager = get_session_manager()
        session = await session_manager.get_session()

        for _ in range(3):
            try:
                async with session.get(url) as resp:
                    response = await resp.json()
                    await self.cache.set_cache(cache_key, response, ttl=600)
                    return response
            except (ClientConnectorError, TimeoutError):
                await sleep(5)
            except (JSONDecodeError, ContentTypeError):
                return {}
        return {}

    def extract_raw_versions(
        self, metadata: dict[str, Any] | list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        raw_versions = []
        if isinstance(metadata, list):
            for version_data in metadata:
                version = version_data.get("number")
                created_at = version_data.get("created_at")
                if version:
                    raw_versions.append({"name": version, "release_date": created_at})
        return raw_versions

    async def get_versions(
        self, metadata: dict[str, Any] | list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        if not metadata:
            return []
        raw = self.extract_raw_versions(metadata)
        return self.orderer.order_versions(raw)

    def get_repo_url(
        self, metadata: dict[str, Any] | list[dict[str, Any]]
    ) -> str | None:
        if not metadata:
            return None

        if isinstance(metadata, list):
            for version_data in metadata:
                version_metadata: dict = version_data.get("metadata", {})
                raw_url = (
                    version_metadata.get("source_code_uri")
                    or version_metadata.get("homepage_uri")
                    or version_metadata.get("bug_tracker_uri")
                )

                if raw_url:
                    norm_url = self.repo_normalizer.normalize(raw_url)
                    if norm_url and self.repo_normalizer.check():
                        return norm_url

        return None

    def get_package_requirements(self, metadata: dict[str, Any]) -> dict[str, Any]:
        requirements: dict[str, Any] = {}

        if not metadata:
            return requirements

        dependencies = metadata.get("dependencies", {})
        runtime_deps = dependencies.get("runtime", []) or []

        for dep in runtime_deps:
            name = dep.get("name")
            req = dep.get("requirements")
            if name:
                if req:
                    req = req.replace("==", "=")
                requirements[name.lower()] = req or ""

        return requirements

    async def extract_import_names(self, gem_name: str, version: str) -> list[str]:
        cache_key = f"import_names:{gem_name}:{version}"
        cached = await self.cache.get_cache(cache_key)
        if cached:
            logger.info(
                f"RubyGems - import_names para {gem_name}@{version} obtenidos de cache"
            )
            return cached

        try:
            logger.info(f"RubyGems - Extrayendo import_names de {gem_name}@{version}")

            url = f"{self.DOWNLOAD_URL}/{gem_name}-{version}.gem"
            session_manager = get_session_manager()
            session = await session_manager.get_session()

            async with session.get(url, timeout=get_default_timeout()) as resp:
                if resp.status != 200:
                    logger.warning(
                        f"RubyGems - No se pudo descargar {gem_name}@{version}: HTTP {resp.status}"
                    )
                    return []

                gem_bytes = await resp.read()

            import_names = await to_thread(self.extract_from_gem, gem_bytes)

            if import_names:
                await self.cache.set_cache(cache_key, import_names, ttl=604800)
                logger.info(
                    f"RubyGems - {len(import_names)} import_names extraídos de {gem_name}@{version}"
                )
            else:
                logger.warning(
                    f"RubyGems - No se encontraron import_names en {gem_name}@{version}"
                )

            return import_names

        except TimeoutError:
            logger.error(f"RubyGems - Timeout al descargar {gem_name}@{version}")
            return []
        except Exception as e:
            logger.error(
                f"RubyGems - Error extrayendo import_names de {gem_name}@{version}: {e}"
            )
            return []

    def extract_from_gem(self, gem_bytes: bytes) -> list[str]:
        import_names = set()

        try:
            with open_tarfile(fileobj=BytesIO(gem_bytes), mode="r") as gem_tar:
                data_tar_gz = None
                for member in gem_tar.getmembers():
                    if member.name.endswith("data.tar.gz"):
                        data_tar_file = gem_tar.extractfile(member)
                        if data_tar_file:
                            data_tar_gz = data_tar_file.read()
                            break

                if not data_tar_gz:
                    return []

                with open_tarfile(
                    fileobj=BytesIO(data_tar_gz), mode="r:gz"
                ) as data_tar:
                    for member in data_tar.getmembers():
                        if not member.isfile():
                            continue

                        if member.name.startswith("lib/") and member.name.endswith(
                            ".rb"
                        ):
                            if "_spec.rb" in member.name or "_test.rb" in member.name:
                                continue

                            path = member.name[4:]
                            import_name = path[:-3]

                            ruby_style = import_name.replace("/", "::")
                            import_names.add(ruby_style)

            return sorted(import_names)

        except TarError as e:
            logger.error(f"RubyGems - Error de tarball: {e}")
            return []
        except Exception as e:
            logger.error(f"RubyGems - Error inesperado en extracción: {e}")
            return []
