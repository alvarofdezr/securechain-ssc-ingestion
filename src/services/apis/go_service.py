from __future__ import annotations

import io
import re
import zipfile
from asyncio import sleep
from json import JSONDecodeError, loads
from typing import Any

from aiohttp import ClientConnectorError

from src.dependencies import (
    get_cache_manager,
    get_orderer,
    get_session_manager,
)
from src.logger import logger


class GoService:
    """HTTP client service for Go module proxy and index APIs.

    Encapsulates all network interactions required to enumerate Go packages,
    retrieve version lists, fetch version metadata, and download go.mod files
    for transitive dependency resolution. All responses are cached via
    CacheManager to reduce redundant network calls across the Dagster pipeline.

    External endpoints:
    - Index: https://index.golang.org/index (new module stream)
    - Proxy: https://proxy.golang.org (version info and go.mod)
    
    Attributes:
        cache: Cache manager instance for response caching.
        PROXY_URL: Base URL for the Go module proxy.
        INDEX_URL: Base URL for the Go module index.
        orderer: Version orderer for semantic versioning and serial number assignment.
        MAX_ZIP_SIZE: Maximum allowed size for module .zip archives (50 MB).
    """

    def __init__(self) -> None:
        """Initializes the Go service with cache, HTTP session, and versioning utilities."""
        self.cache = get_cache_manager("go")
        self.PROXY_URL = "https://proxy.golang.org"
        self.INDEX_URL = "https://index.golang.org/index"
        self.orderer = get_orderer("GoPackage")
        self.MAX_ZIP_SIZE = 50 * 1024 * 1024

    async def fetch_all_package_names(self, limit: int = 2000) -> list[str]:
        """Retrieves a deduplicated list of module paths from the Go module index.
        
        Fetches a fixed-size batch from the start of the index. For incremental,
        cursor-based ingestion that resumes on failure, use fetch_packages_since instead.
        
        The index endpoint returns newline-delimited JSON (NDJSON), where each line
        is a JSON object with at minimum a 'Path' field. Results are cached for one
        hour to avoid repeated requests across pipeline runs.
        
        Args:
            limit: Maximum number of index entries to fetch in a single request (default: 2000).
        
        Returns:
            A deduplicated list of module path strings. Returns empty list on any
            network or parse failure.
        
        Raises:
            No exceptions raised. Errors are logged and result in returning an empty list.
        """
        cached = await self.cache.get_cache("all_go_packages")
        if cached:
            return cached

        url = f"{self.INDEX_URL}?limit={limit}"
        session_manager = get_session_manager()
        session = await session_manager.get_session()

        try:
            async with session.get(url) as resp:
                text = await resp.text()
                package_names: set[str] = set()
                for line in text.splitlines():
                    if not line:
                        continue
                    try:
                        entry = loads(line)
                        if "Path" in entry:
                            package_names.add(entry["Path"])
                    except (JSONDecodeError, KeyError):
                        continue

                result_list = list(package_names)
                await self.cache.set_cache("all_go_packages", result_list, ttl=3600)
                return result_list

        except ClientConnectorError as e:
            logger.error(f"GoService - Connection error with Index: {e}")
            return []
        except Exception as e:
            logger.error(f"GoService - Unexpected error fetching names: {e}")
            return []

    async def fetch_packages_since(
        self, since: str, limit: int = 2000
    ) -> tuple[list[str], str]:
        """Fetches a batch of module updates from the Go index since a given timestamp.
        
        Uses the ?since=<timestamp> query parameter to retrieve a chronological stream
        of module publications, enabling incremental, cursor-based ingestion that is
        resumable and avoids reprocessing the entire index.
        
        Safe against infinite loops: if the response contains fewer entries than limit,
        the caller should treat the batch as the final one and stop. If last_timestamp
        equals since, an empty result is returned to prevent cursor stalling.
        
        Args:
            since: RFC3339 timestamp string (e.g., '2019-04-10T19:08:52Z').
            limit: Maximum number of entries to return (default: 2000).
        
        Returns:
            A tuple of:
            - List of unique module path strings from the batch.
            - Timestamp of the last entry as the next cursor. Empty string if batch
              is empty or cursor did not advance.
        
        Raises:
            No exceptions raised. Connection errors are logged and result in
            returning empty tuple components.
        """
        url = f"{self.INDEX_URL}?since={since}&limit={limit}"
        session_manager = get_session_manager()
        session = await session_manager.get_session()

        try:
            async with session.get(url) as resp:
                if resp.status != 200:
                    logger.warning(
                        f"GoService - Index returned HTTP {resp.status} for since={since}"
                    )
                    return [], ""

                text = await resp.text()
                package_names: set[str] = set()
                last_timestamp = ""
                lines = [line for line in text.splitlines() if line.strip()]

                for line in lines:
                    try:
                        entry = loads(line)
                        if "Path" in entry:
                            package_names.add(entry["Path"])
                        if "Timestamp" in entry:
                            last_timestamp = entry["Timestamp"]
                    except (JSONDecodeError, KeyError):
                        continue

                if not last_timestamp:
                    return [], since

                if len(lines) < limit:
                    return list(package_names), last_timestamp

                return list(package_names), last_timestamp

        except ClientConnectorError as e:
            logger.error(f"GoService - Connection error with Index: {e}")
            return [], ""
        except Exception as e:
            logger.error(f"GoService - Unexpected error in fetch_packages_since: {e}")
            return [], ""

    async def fetch_versions_list(self, package_name: str) -> list[str]:
        """Fetches the list of tagged versions available for a module from the Go proxy.
        
        Queries the /@v/list endpoint, which returns a plain-text newline-separated
        list of version strings. Pseudo-versions and pre-release tags are included
        as-is. Results are cached for 10 minutes given infrequent version publication
        relative to the ingestion cycle.
        
        HTTP 404 or 410 responses indicate the module is unavailable (retracted or
        unpublished) and are treated as empty version lists rather than errors.
        
        Args:
            package_name: Canonical module path (e.g., 'github.com/gin-gonic/gin').
        
        Returns:
            List of version strings (e.g., ['v1.9.0', 'v1.8.2']). Returns empty list
            if module not found or on repeated network failure after three retry attempts.
        
        Raises:
            No exceptions raised. Errors are logged and result in returning an empty list.
        """
        package_name_lower = package_name.lower()
        cached = await self.cache.get_cache(f"versions_list:{package_name_lower}")
        if cached:
            return cached

        url = f"{self.PROXY_URL}/{package_name_lower}/@v/list"
        session_manager = get_session_manager()
        session = await session_manager.get_session()

        for _ in range(3):
            try:
                async with session.get(url) as resp:
                    if resp.status in (404, 410):
                        return []
                    text = await resp.text()
                    versions = [v.strip() for v in text.splitlines() if v.strip()]
                    await self.cache.set_cache(
                        f"versions_list:{package_name_lower}", versions, ttl=600
                    )
                    return versions
            except (ClientConnectorError, TimeoutError):
                await sleep(2)

        return []

    async def fetch_version_info(
        self, package_name: str, version: str
    ) -> dict[str, Any]:
        """Fetches version metadata from the Go proxy /@v/{version}.info endpoint.
        
        Retrieves version metadata including 'Version' and 'Time' fields. The 'Time'
        field carries the canonical release timestamp and is used to populate
        release_date in the dependency graph. Results are cached for one hour.
        
        Args:
            package_name: Canonical module path.
            version: Version string, with or without the leading 'v' prefix.
        
        Returns:
            Dictionary with proxy response fields. Returns empty dict if version
            not found (404/410) or on network failure.
        
        Raises:
            No exceptions raised. Errors are logged and result in returning an empty dict.
        """
        package_name_lower = package_name.lower()
        req_version = version if version.startswith("v") else f"v{version}"
        cache_key = f"{package_name_lower}:{req_version}:info"

        cached = await self.cache.get_cache(cache_key)
        if cached:
            return cached

        url = f"{self.PROXY_URL}/{package_name_lower}/@v/{req_version}.info"
        session_manager = get_session_manager()
        session = await session_manager.get_session()

        for _ in range(3):
            try:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        await self.cache.set_cache(cache_key, data, ttl=3600)
                        return data
                    if resp.status in (404, 410):
                        return {}
            except (ClientConnectorError, TimeoutError):
                await sleep(2)
            except Exception:
                return {}

        return {}

    async def get_versions(self, package_name: str) -> list[dict[str, Any]]:
        """Retrieves an ordered list of version descriptors for a module.
        
        Fetches raw version strings from the proxy and categorizes them into tagged
        releases and pseudo-versions. If tagged releases exist, only those are processed.
        If no tagged releases exist, pseudo-versions are included to ensure the module
        is not omitted from the graph.
        
        The final list is delegated to an Orderer instance for semantic version sorting
        and serial number assignment.
        
        Args:
            package_name: Canonical module path.
        
        Returns:
            List of version descriptor dicts with 'name', 'release_date', and
            'serial_number' keys, ordered by ascending semantic version.
            Returns empty list if no versions available.
        
        Raises:
            Exception: Propagates exceptions from Orderer operations.
        """
        raw_version_strings = await self.fetch_versions_list(package_name)
        if not raw_version_strings:
            return []

        tagged_versions = []
        pseudo_versions = []
        for v_str in raw_version_strings:
            if self._is_pseudo_version(v_str):
                pseudo_versions.append(v_str)
            else:
                tagged_versions.append(v_str)

        versions_to_process = tagged_versions + pseudo_versions

        formatted_versions = [
            {"name": v_str, "release_date": None} for v_str in set(versions_to_process)
        ]
        return self.orderer.order_versions(formatted_versions)

    @staticmethod
    def _is_pseudo_version(version_str: str) -> bool:
        """Determines if a version string is a Go pseudo-version.
        
        Go pseudo-versions are generated for commits without version tags and follow
        a strict format: vX.Y.Z-yyyymmddhhmmss-commit. This method distinguishes
        them from standard pre-release tags (e.g., v1.0.0-beta.1), which should
        not be filtered.
        
        Args:
            version_str: The version string to check.
        
        Returns:
            True if the string matches the pseudo-version pattern, False otherwise.
        """
        pseudo_version_pattern = r"^v\d+\.\d+\.\d+-\d{14}-[a-f0-9]{12}$"
        return re.match(pseudo_version_pattern, version_str) is not None

    def get_repo_url(self, package_name: str) -> str:
        """Derives the source repository URL for a module from its path.
        
        For modules hosted on well-known VCS platforms (github.com, gitlab.com),
        constructs the repository URL directly from the module path. For all other
        modules, uses the canonical pkg.go.dev documentation URL as fallback,
        which is publicly accessible and valid for any module via the Go proxy.
        
        Args:
            package_name: Canonical module path.
        
        Returns:
            Fully qualified HTTPS URL string.
        """
        if package_name.startswith("github.com") or package_name.startswith(
            "gitlab.com"
        ):
            return f"https://{package_name}"
        return f"https://pkg.go.dev/{package_name}"

    async def get_import_names(self, module_path: str, version: str) -> list[str]:
        """Extracts the list of importable package paths from a Go module's source.
        
        Downloads the module's .zip archive from the proxy and inspects it in-memory
        to identify all directories containing at least one non-test .go file. The
        import path for each directory is constructed by joining the module path
        with the directory's relative path inside the zip.
        
        Archives larger than MAX_ZIP_SIZE are skipped with a warning to prevent
        memory exhaustion. Network failures and zip parsing errors fall back safely
        to returning the module path itself.
        
        Args:
            module_path: Canonical module path.
            version: Version string of the module to inspect.
        
        Returns:
            Sorted list of unique import paths. Returns [module_path] as safe fallback
            if zip cannot be fetched, is invalid, or exceeds size limits.
        
        Raises:
            No exceptions raised. Errors are logged and result in fallback return.
        """
        module_path_lower = module_path.lower()
        cache_key = f"import_names:{module_path_lower}:{version}"
        cached = await self.cache.get_cache(cache_key)
        if cached:
            return cached

        url = f"{self.PROXY_URL}/{module_path_lower}/@v/{version}.zip"
        session_manager = get_session_manager()
        session = await session_manager.get_session()

        try:
            async with session.head(url) as resp:
                if resp.status != 200:
                    return [module_path]
                size = int(resp.headers.get("Content-Length", 0))
                if size > self.MAX_ZIP_SIZE:
                    logger.warning(
                        f"GoService - Skipping large module zip: {module_path}@{version} ({size} bytes)"
                    )
                    return [module_path]

            async with session.get(url) as resp:
                if resp.status != 200:
                    return [module_path]

                zip_bytes = await resp.read()
                zip_file = zipfile.ZipFile(io.BytesIO(zip_bytes))

                importable_dirs: set[str] = set()
                prefix = f"{module_path}@{version}/"

                for name in zip_file.namelist():
                    if (
                        not name.startswith(prefix)
                        or not name.endswith(".go")
                        or name.endswith("_test.go")
                    ):
                        continue

                    dir_name = "/".join(name[len(prefix) :].split("/")[:-1])
                    importable_dirs.add(dir_name)

                import_names: set[str] = {module_path}
                for dir_path in importable_dirs:
                    if dir_path:
                        import_names.add(f"{module_path}/{dir_path}")

                result = sorted(import_names)
                await self.cache.set_cache(cache_key, result, ttl=604800)
                return result

        except (ClientConnectorError, zipfile.BadZipFile, Exception) as e:
            logger.error(
                f"GoService - Failed to get import names for {module_path}@{version}: {e}"
            )
            return [module_path]

    async def get_package_requirements(
        self, package_name: str, version: str
    ) -> dict[str, str]:
        """Downloads and parses the go.mod file for a specific module version.
        
        Queries the /@v/{version}.mod endpoint, which returns raw go.mod content.
        Parses the file to extract direct and indirect require directives, used
        during transitive dependency extraction to populate child package nodes
        in the knowledge graph.
        
        Args:
            package_name: Canonical module path of the parent module.
            version: Tagged version string for which to fetch the go.mod file.
        
        Returns:
            Dictionary mapping required module paths to their declared version strings.
            Returns empty dict if file cannot be fetched or parsed.
        
        Raises:
            No exceptions raised. Errors are logged and result in returning an empty dict.
        """
        package_name_lower = package_name.lower()
        req_version = version if version.startswith("v") else f"v{version}"
        url = f"{self.PROXY_URL}/{package_name_lower}/@v/{req_version}.mod"

        session_manager = get_session_manager()
        session = await session_manager.get_session()

        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    content = await resp.text()
                    return self._parse_go_mod(content)
                if resp.status in (404, 410):
                    return {}
        except Exception as e:
            logger.error(
                f"GoService - Error fetching go.mod for {package_name}@{version}: {e}"
            )

        return {}

    def _parse_go_mod(self, content: str) -> dict[str, str]:
        """Parses require directives from raw go.mod content.
        
        Handles both block form (require (...)) and single-line form (require ...).
        Inline comments (e.g., '// indirect') are stripped to prevent annotation
        tokens from being captured as part of the version string.
        
        Args:
            content: Raw text content of a go.mod file.
        
        Returns:
            Mapping of module paths to their declared version strings.
        """
        dependencies: dict[str, str] = {}

        block_pattern = r"require\s*\((.*?)\)"
        for block in re.finditer(block_pattern, content, re.DOTALL):
            for line in block.group(1).splitlines():
                line = line.strip()
                if not line or line.startswith("//"):
                    continue
                if "//" in line:
                    line = line.split("//")[0].strip()
                parts = line.split()
                if len(parts) >= 2:
                    dependencies[parts[0]] = parts[1]

        single_pattern = r"^\s*require\s+([^(\s]\S*)\s+(\S+)"
        for match in re.finditer(single_pattern, content, re.MULTILINE):
            version = match.group(2).split("//")[0].strip()
            dependencies[match.group(1)] = version

        return dependencies
