from asyncio import sleep, to_thread
from io import BytesIO
from json import JSONDecodeError
from tarfile import open as open_tarfile
from typing import Any
from zipfile import ZipFile

from aiohttp import ClientConnectorError, ContentTypeError
from regex import findall

from src.dependencies import (
    get_cache_manager,
    get_default_timeout,
    get_orderer,
    get_pypi_constraints_parser,
    get_repo_normalizer,
    get_session_manager,
)
from src.logger import logger


class PyPIService:
    def __init__(self):
        self.cache = get_cache_manager("pypi")
        self.BASE_URL = "https://pypi.python.org/pypi"
        self.SIMPLE_URL = "https://pypi.org/simple"
        self.FILES_URL = "https://files.pythonhosted.org/packages"
        self.orderer = get_orderer("PyPIPackage")
        self.repo_normalizer = get_repo_normalizer()
        self.pypi_constraints_parser = get_pypi_constraints_parser()

    async def fetch_all_package_names(self) -> list[str]:
        cached = await self.cache.get_cache("all_pypi_packages")
        if cached:
            return cached

        url = f"{self.SIMPLE_URL}/"
        session_manager = get_session_manager()
        session = await session_manager.get_session()

        try:
            async with session.get(url) as resp:
                html = await resp.text()
                pattern = r'<a href="[^"]*">([^<]+)</a>'
                package_names = findall(pattern, html)
                await self.cache.set_cache("all_pypi_packages", package_names, ttl=3600)
                return package_names
        except ClientConnectorError as e:
            logger.error(f"PyPI - Connection error: {e}")
            return []
        except TimeoutError as e:
            logger.error(f"PyPI - Timeout error: {e}")
            return []
        except Exception as e:
            logger.error(f"PyPI - Unexpected error fetching package names: {e}")
            return []

    async def fetch_package_metadata(self, package_name: str) -> dict[str, Any]:
        cached = await self.cache.get_cache(package_name)
        if cached:
            return cached

        url = f"{self.BASE_URL}/{package_name}/json"
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

        url = f"{self.BASE_URL}/{package_name}/{version_name}/json"
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

    def extract_raw_versions(self, metadata: dict[str, Any]) -> list[dict[str, Any]]:
        raw_versions = []
        for version, files in (metadata.get("releases") or {}).items():
            upload_time = None
            if files:
                f = files[0]
                upload_time = f.get("upload_time_iso_8601") or f.get("upload_time")
            raw_versions.append({"name": version, "release_date": upload_time})
        return raw_versions

    async def get_versions(self, metadata: dict[str, Any]) -> list[dict[str, Any]]:
        if not metadata:
            return []
        raw = self.extract_raw_versions(metadata)
        return self.orderer.order_versions(raw)

    def get_repo_url(self, metadata: dict[str, Any]) -> str | None:
        if not metadata:
            return None

        info = metadata.get("info", {})
        raw_url = info.get("home_page")
        norm_url = self.repo_normalizer.normalize(raw_url)
        if self.repo_normalizer.check():
            return norm_url

        project_urls = info.get("project_urls") or {}
        for _, raw_url in project_urls.items():
            norm_url = self.repo_normalizer.normalize(raw_url)
            if self.repo_normalizer.check():
                return norm_url

        return None

    def get_package_requirements(self, metadata: dict[str, Any]) -> dict[str, Any]:
        requirements: dict[str, Any] = {}

        for dependency in metadata.get("info", {}).get("requires_dist", []) or []:
            data = dependency.split(";")

            if "python-version" in data[0]:
                continue
            if len(data) > 1 and "extra" in data[1]:
                continue
            if len(data) > 1:
                python_version = any(
                    sub in data[1]
                    for sub in [
                        '== "3.10"',
                        '<= "3.10"',
                        '>= "3.10"',
                        '>= "3"',
                        '<= "3"',
                        '>= "2',
                        '> "2',
                    ]
                )
                if "python_version" in data[1] and not python_version:
                    continue

            if "[" in data[0]:
                pos_1 = self.pypi_constraints_parser.get_first_position(data[0], ["["])
                pos_2 = (
                    self.pypi_constraints_parser.get_first_position(data[0], ["]"]) + 1
                )
                data[0] = data[0][:pos_1] + data[0][pos_2:]

            data = (
                data[0]
                .replace("(", "")
                .replace(")", "")
                .replace(" ", "")
                .replace("'", "")
            )
            pos = self.pypi_constraints_parser.get_first_position(
                data, ["<", ">", "=", "!", "~"]
            )
            requirements[data[:pos].lower()] = self.pypi_constraints_parser.parse(
                data[pos:]
            )

        return requirements

    async def extract_import_names(self, package_name: str, version: str) -> list[str]:
        cache_key = f"import_names:{package_name}:{version}"
        cached = await self.cache.get_cache(cache_key)
        if cached:
            logger.info(
                f"PyPI - import_names para {package_name}@{version} obtenidos de cache"
            )
            return cached

        try:
            logger.info(f"PyPI - Extrayendo import_names de {package_name}@{version}")

            metadata = await self.fetch_package_version_metadata(package_name, version)
            if not metadata:
                logger.warning(
                    f"PyPI - No se encontró metadata para {package_name}@{version}"
                )
                return []

            urls = metadata.get("urls", [])
            download_url = None
            file_type = None

            for url_info in urls:
                if url_info.get("packagetype") == "bdist_wheel":
                    download_url = url_info.get("url")
                    file_type = "wheel"
                    break
                elif url_info.get("packagetype") == "sdist":
                    download_url = url_info.get("url")
                    file_type = "sdist"

            if not download_url:
                logger.warning(
                    f"PyPI - No se encontró archivo descargable para {package_name}@{version}"
                )
                return []

            session_manager = get_session_manager()
            session = await session_manager.get_session()
            async with session.get(download_url, timeout=get_default_timeout()) as resp:
                if resp.status != 200:
                    logger.warning(
                        f"PyPI - No se pudo descargar {package_name}@{version}: HTTP {resp.status}"
                    )
                    return []

                package_bytes = await resp.read()

            import_names = await to_thread(
                self.extract_from_package, package_bytes, file_type, package_name
            )

            if import_names:
                await self.cache.set_cache(cache_key, import_names, ttl=604800)
                logger.info(
                    f"PyPI - {len(import_names)} import_names extraídos de {package_name}@{version}"
                )
            else:
                logger.warning(
                    f"PyPI - No se encontraron import_names en {package_name}@{version}"
                )

            return import_names

        except TimeoutError:
            logger.error(f"PyPI - Timeout al descargar {package_name}@{version}")
            return []
        except Exception as e:
            logger.error(
                f"PyPI - Error extrayendo import_names de {package_name}@{version}: {e}"
            )
            return []

    def extract_from_package(
        self, package_bytes: bytes, file_type: str | None, package_name: str
    ) -> list[str]:
        import_names = set()

        try:
            if file_type == "wheel":
                with ZipFile(BytesIO(package_bytes)) as whl_zip:
                    for file_info in whl_zip.namelist():
                        if file_info.endswith(".py") and "/" in file_info:
                            parts = file_info.split("/")
                            if any(
                                skip in parts
                                for skip in [
                                    "test",
                                    "tests",
                                    "example",
                                    "examples",
                                    "docs",
                                ]
                            ):
                                continue

                            if len(parts) >= 2:
                                module_path = ".".join(parts[:-1])
                                if module_path:
                                    import_names.add(module_path)
                                    import_names.add(parts[0])

            else:
                try:
                    with open_tarfile(
                        fileobj=BytesIO(package_bytes), mode="r:gz"
                    ) as tar:
                        for member in tar.getmembers():
                            if member.name.endswith(".py") and "/" in member.name:
                                parts = member.name.split("/")
                                if any(
                                    skip in parts
                                    for skip in [
                                        "test",
                                        "tests",
                                        "example",
                                        "examples",
                                        "docs",
                                    ]
                                ):
                                    continue

                                if len(parts) >= 3 and parts[-1] == "__init__.py":
                                    import_names.add(parts[-2])
                except Exception:
                    try:
                        with ZipFile(BytesIO(package_bytes)) as sdist_zip:
                            for file_info in sdist_zip.namelist():
                                if file_info.endswith(".py") and "/" in file_info:
                                    parts = file_info.split("/")
                                    if any(
                                        skip in parts
                                        for skip in [
                                            "test",
                                            "tests",
                                            "example",
                                            "examples",
                                            "docs",
                                        ]
                                    ):
                                        continue

                                    if len(parts) >= 3 and parts[-1] == "__init__.py":
                                        import_names.add(parts[-2])
                    except Exception:
                        pass

            if not import_names:
                normalized_name = package_name.replace("-", "_").lower()
                import_names.add(normalized_name)

            return sorted(import_names)

        except Exception as e:
            logger.error(f"PyPI - Error inesperado en extracción: {e}")
            normalized_name = package_name.replace("-", "_").lower()
            return [normalized_name]
