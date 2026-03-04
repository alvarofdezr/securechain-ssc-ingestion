from asyncio import sleep, to_thread
from io import BytesIO
from json import JSONDecodeError
from tarfile import ReadError
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


class NPMService:
    def __init__(self):
        self.cache = get_cache_manager("npm")
        self.BASE_URL = "https://registry.npmjs.org"
        self.CHANGES_URL = "https://replicate.npmjs.com/_changes"
        self.orderer = get_orderer("NPMPackage")
        self.repo_normalizer = get_repo_normalizer()

    async def fetch_all_package_names(self) -> list[str]:
        cached = await self.cache.get_cache("all_npm_packages")
        if cached:
            return cached

        session_manager = get_session_manager()
        session = await session_manager.get_session()
        all_packages = set()

        try:
            batch_size = 10000
            since = 0

            logger.info(
                f"NPM - Fetching all packages using _changes feed (batch size: {batch_size})"
            )

            while True:
                params = {"limit": batch_size, "since": since}

                async with session.get(
                    self.CHANGES_URL, params=params, timeout=get_long_timeout()
                ) as resp:
                    if resp.status != 200:
                        logger.warning(
                            f"NPM - Error fetching changes: HTTP {resp.status}"
                        )
                        break

                    data = await resp.json()
                    results = data.get("results", [])
                    last_seq = data.get("last_seq")

                    if not results:
                        break

                    all_packages.update(
                        change["id"]
                        for change in results
                        if "id" in change and not change["id"].startswith("_")
                    )

                    if len(results) < batch_size:
                        break

                    since = last_seq or results[-1].get("seq", since)

            package_names = sorted(all_packages)
            logger.info(f"NPM - Successfully fetched {len(package_names):,} packages")

            await self.cache.set_cache("all_npm_packages", package_names, ttl=3600)
            return package_names

        except (
            ClientConnectorError,
            TimeoutError,
            JSONDecodeError,
            ContentTypeError,
            Exception,
        ) as e:
            logger.error(f"NPM - Error fetching package names: {e}")
            return []

    async def fetch_package_metadata(self, package_name: str) -> dict[str, Any]:
        cached = await self.cache.get_cache(package_name)
        if cached:
            return cached

        url = f"{self.BASE_URL}/{package_name}"
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

    def extract_raw_versions_and_requirements(
        self, metadata: dict[str, Any]
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        raw_versions = []
        requirements = []
        versions_data = metadata.get("versions", {})
        time_data = metadata.get("time", {})

        for version_name, version_info in versions_data.items():
            release_date = time_data.get(version_name)
            raw_versions.append({"name": version_name, "release_date": release_date})
            dependencies = version_info.get("dependencies", {})
            requirements.append(dependencies)

        return raw_versions, requirements

    async def get_versions_and_requirements(
        self, metadata: dict[str, Any]
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        if not metadata:
            return [], []
        raw_versions, requirements = self.extract_raw_versions_and_requirements(
            metadata
        )
        versions = self.orderer.order_versions(raw_versions)
        return versions, requirements

    def get_repo_url(self, metadata: dict[str, Any]) -> str | None:
        if not metadata:
            return None

        raw_url = None
        repository = metadata.get("repository")
        if repository:
            if isinstance(repository, dict):
                raw_url = repository.get("url")
            else:
                raw_url = repository

            if raw_url:
                norm_url = self.repo_normalizer.normalize(raw_url)
                if self.repo_normalizer.check():
                    return norm_url

        homepage = metadata.get("homepage")
        if homepage:
            norm_url = self.repo_normalizer.normalize(homepage)
            if self.repo_normalizer.check():
                return norm_url

        bugs = metadata.get("bugs")
        if bugs and isinstance(bugs, dict):
            bugs_url = bugs.get("url")
            if bugs_url:
                norm_url = self.repo_normalizer.normalize(bugs_url)
                if self.repo_normalizer.check():
                    return norm_url

        return None

    async def extract_import_names(self, package_name: str, version: str) -> list[str]:
        cache_key = f"import_names:{package_name}:{version}"
        cached = await self.cache.get_cache(cache_key)
        if cached:
            return cached

        metadata_url = f"{self.BASE_URL}/{package_name.replace('/', '%2F')}"
        session_manager = get_session_manager()
        session = await session_manager.get_session()

        try:
            async with session.get(metadata_url, timeout=get_default_timeout()) as resp:
                if resp.status != 200:
                    logger.warning(
                        f"NPM - Failed to get metadata for {package_name}@{version}: HTTP {resp.status}"
                    )
                    return []

                metadata = await resp.json()

                versions = metadata.get("versions", {})
                version_data = versions.get(version)

                if not version_data:
                    logger.warning(
                        f"NPM - Version {version} not found for {package_name}"
                    )
                    return []

                tarball_url = version_data.get("dist", {}).get("tarball")

                if not tarball_url:
                    logger.warning(
                        f"NPM - No tarball URL found for {package_name}@{version}"
                    )
                    return []

            async with session.get(tarball_url, timeout=get_default_timeout()) as resp:
                if resp.status != 200:
                    logger.warning(
                        f"NPM - Failed to download {package_name}@{version}: HTTP {resp.status}"
                    )
                    return []

                tgz_bytes = await resp.read()
                import_names = await to_thread(
                    self.extract_from_tarball, package_name, tgz_bytes
                )

                await self.cache.set_cache(cache_key, import_names, ttl=604800)
                return import_names

        except Exception as e:
            logger.error(
                f"NPM - Error extracting import_names for {package_name}@{version}: {e}"
            )
            return []

    def extract_from_tarball(self, package_name: str, tgz_bytes: bytes) -> list[str]:
        import_names = set()

        try:
            with open_tarfile(fileobj=BytesIO(tgz_bytes), mode="r:gz") as tar:
                for member in tar.getmembers():
                    if member.isfile():
                        name = member.name
                        if name.endswith((".js", ".mjs", ".ts")) and name.startswith(
                            "package/"
                        ):
                            path = name[len("package/") :]

                            if (
                                path.startswith("node_modules")
                                or path.startswith("test")
                                or "internal" in path
                            ):
                                continue

                            if path.endswith("index.js"):
                                import_names.add(package_name)
                            else:
                                for ext in [".js", ".mjs", ".ts"]:
                                    if path.endswith(ext):
                                        module = path[: -len(ext)]
                                        break
                                else:
                                    module = path

                                import_name = f"{package_name}/{module}"
                                import_names.add(import_name.replace("\\", "/"))

            return sorted(import_names)

        except ReadError:
            logger.warning(f"NPM - Bad tarball file for {package_name}")
            return []
        except Exception as e:
            logger.error(f"NPM - Unexpected error extracting {package_name}: {e}")
            return []
