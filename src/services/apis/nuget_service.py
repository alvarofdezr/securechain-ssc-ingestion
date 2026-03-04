from asyncio import Lock, Semaphore, gather, sleep, to_thread
from io import BytesIO
from json import JSONDecodeError
from typing import Any
from zipfile import BadZipFile, ZipFile

from aiohttp import ClientConnectorError, ContentTypeError
from regex import search

from src.dependencies import (
    get_cache_manager,
    get_default_timeout,
    get_long_timeout,
    get_medium_timeout,
    get_orderer,
    get_repo_normalizer,
    get_session_manager,
)
from src.logger import logger


class NuGetService:
    def __init__(self):
        self.cache = get_cache_manager("nuget")
        self.BASE_URL = "https://api.nuget.org/v3/registration5-gz-semver2"
        self.INDEX_URL = "https://api.nuget.org/v3/catalog0/index.json"
        self.DOWNLOAD_URL = "https://www.nuget.org/api/v2/package"
        self.orderer = get_orderer("NuGetPackage")
        self.repo_normalizer = get_repo_normalizer()

    async def fetch_all_package_names(self) -> list[str]:
        cached = await self.cache.get_cache("all_nuget_packages")
        if cached:
            return cached

        session_manager = get_session_manager()
        session = await session_manager.get_session()
        all_packages = set()

        try:
            logger.info("NuGet - Fetching all packages using catalog")

            async with session.get(
                self.INDEX_URL, timeout=get_medium_timeout()
            ) as resp:
                if resp.status != 200:
                    logger.error(
                        f"NuGet - Error fetching catalog index: HTTP {resp.status}"
                    )
                    return []

                catalog_index = await resp.json()
                pages = catalog_index.get("items", [])

            logger.info(f"NuGet - Processing {len(pages):,} catalog pages")

            semaphore = Semaphore(50)
            pages_processed = 0
            lock = Lock()

            async def fetch_catalog_page(page_url: str):
                nonlocal pages_processed

                async with semaphore:
                    try:
                        async with session.get(
                            page_url, timeout=get_long_timeout()
                        ) as resp:
                            if resp.status != 200:
                                return

                            page_data = await resp.json()
                            items = page_data.get("items", [])

                            page_packages = {
                                item.get("nuget:id")
                                for item in items
                                if item.get("nuget:id")
                            }

                            async with lock:
                                all_packages.update(page_packages)
                                pages_processed += 1

                                if pages_processed % 500 == 0:
                                    logger.info(
                                        f"NuGet - Pages processed: {pages_processed:,} | "
                                        f"Unique packages: {len(all_packages):,}"
                                    )

                    except (TimeoutError, Exception) as e:
                        logger.debug(f"NuGet - Error fetching catalog page: {e}")

            tasks = [
                fetch_catalog_page(page.get("@id")) for page in pages if page.get("@id")
            ]

            await gather(*tasks, return_exceptions=True)

            package_names = sorted(all_packages)
            logger.info(
                f"NuGet - Successfully fetched {len(package_names):,} unique packages"
            )

            await self.cache.set_cache("all_nuget_packages", package_names, ttl=3600)
            return package_names

        except Exception as e:
            logger.error(f"NuGet - Fatal error in fetch_all_package_names: {e}")
            return []

    async def fetch_page_versions(self, url: str) -> list[dict[str, Any]]:
        cached = await self.cache.get_cache(url)
        if cached:
            return cached

        session_manager = get_session_manager()
        session = await session_manager.get_session()
        for _ in range(3):
            try:
                async with session.get(url) as resp:
                    if resp.status == 404:
                        return []
                    response = await resp.json()
                    items = response.get("items", [])
                    await self.cache.set_cache(url, items, ttl=600)
                    return items
            except (ClientConnectorError, TimeoutError):
                await sleep(5)
            except (JSONDecodeError, ContentTypeError):
                return []
        return []

    async def fetch_package_metadata(self, package_name: str) -> dict[str, Any]:
        cached = await self.cache.get_cache(package_name)
        if cached:
            return cached

        url = f"{self.BASE_URL}/{package_name}/index.json"
        session_manager = get_session_manager()
        session = await session_manager.get_session()

        for _ in range(3):
            try:
                async with session.get(url) as resp:
                    if resp.status == 404:
                        return {}
                    response = await resp.json()
                    await self.cache.set_cache(package_name, response, ttl=600)
                    return response
            except (ClientConnectorError, TimeoutError):
                await sleep(5)
            except (JSONDecodeError, ContentTypeError):
                return {}
        return {}

    async def extract_raw_versions_and_requirements(
        self, metadata: dict[str, Any]
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        raw_versions = []
        requirements = []

        for item in metadata.get("items", []):
            subitems = item.get("items") or await self.fetch_page_versions(
                item.get("@id", "")
            )
            for version_item in subitems:
                catalog_entry = version_item.get("catalogEntry", {})
                name = catalog_entry.get("version")
                release_date = catalog_entry.get("published")

                if name:
                    raw_versions.append({"name": name, "release_date": release_date})

                    dependencies = {
                        dep.get("id"): dep.get("range")
                        for group in catalog_entry.get("dependencyGroups", [])
                        if "targetFramework" not in group
                        for dep in group.get("dependencies", [])
                    }
                    requirements.append(dependencies)

        return raw_versions, requirements

    async def get_versions_and_requirements(
        self, metadata: dict[str, Any]
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        if not metadata:
            return [], []
        raw_versions, requirements = await self.extract_raw_versions_and_requirements(
            metadata
        )
        versions = self.orderer.order_versions(raw_versions)
        return versions, requirements

    async def get_repo_url(self, metadata: dict[str, Any]) -> str | None:
        if not metadata:
            return None

        for item in metadata.get("items", []):
            subitems = item.get("items") or await self.fetch_page_versions(
                item.get("@id", "")
            )
            for version_item in subitems:
                catalog_entry = version_item.get("catalogEntry", {})
                raw_url = catalog_entry.get("repositoryUrl")

                if raw_url:
                    norm_url = self.repo_normalizer.normalize(raw_url)
                    if norm_url and self.repo_normalizer.check():
                        return norm_url

        return None

    async def extract_import_names(self, package_name: str, version: str) -> list[str]:
        cache_key = f"import_names:{package_name}:{version}"
        cached = await self.cache.get_cache(cache_key)
        if cached:
            logger.info(
                f"NuGet - import_names para {package_name}@{version} obtenidos de cache"
            )
            return cached

        try:
            logger.info(f"NuGet - Extrayendo import_names de {package_name}@{version}")

            url = f"{self.DOWNLOAD_URL}/{package_name}/{version}"
            from src.dependencies import get_session_manager

            session_manager = get_session_manager()
            session = await session_manager.get_session()

            async with session.get(url, timeout=get_default_timeout()) as resp:
                if resp.status != 200:
                    logger.warning(
                        f"NuGet - No se pudo descargar {package_name}@{version}: HTTP {resp.status}"
                    )
                    return []

                nupkg_bytes = await resp.read()

            import_names = await to_thread(
                self.extract_from_nupkg, nupkg_bytes, package_name
            )

            if import_names:
                await self.cache.set_cache(cache_key, import_names, ttl=604800)
                logger.info(
                    f"NuGet - {len(import_names)} import_names extraídos de {package_name}@{version}"
                )
            else:
                logger.warning(
                    f"NuGet - No se encontraron import_names en {package_name}@{version}"
                )

            return import_names

        except TimeoutError:
            logger.error(f"NuGet - Timeout al descargar {package_name}@{version}")
            return []
        except Exception as e:
            logger.error(
                f"NuGet - Error extrayendo import_names de {package_name}@{version}: {e}"
            )
            return []

    def extract_from_nupkg(self, nupkg_bytes: bytes, package_name: str) -> list[str]:
        import_names = set()

        try:
            with ZipFile(BytesIO(nupkg_bytes)) as nupkg_zip:
                for file_info in nupkg_zip.namelist():
                    if file_info.startswith("lib/") and file_info.endswith(".dll"):
                        dll_name = file_info.split("/")[-1][:-4]
                        if dll_name in [
                            "System",
                            "Microsoft.CSharp",
                            "netstandard",
                            "mscorlib",
                        ]:
                            continue
                        import_names.add(dll_name)

                for file_info in nupkg_zip.namelist():
                    if file_info.endswith(".nuspec"):
                        try:
                            nuspec_content = nupkg_zip.read(file_info).decode("utf-8")
                            id_match = search(r"<id>([^<]+)</id>", nuspec_content)
                            if id_match:
                                package_id = id_match.group(1)
                                import_names.add(package_id)
                        except Exception:
                            pass

            if not import_names and package_name:
                import_names.add(package_name)

            return sorted(import_names)

        except BadZipFile as e:
            logger.error(f"NuGet - Error de ZIP: {e}")
            return []
        except Exception as e:
            logger.error(f"NuGet - Error inesperado en extracción: {e}")
            return []
