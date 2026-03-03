from __future__ import annotations

import io
import re
import zipfile
from asyncio import sleep
from json import JSONDecodeError, loads
from typing import Any

from aiohttp import ClientConnectorError, ContentTypeError

from src.dependencies import (
    get_cache_manager,
    get_orderer,
    get_session_manager,
)
from src.logger import logger


class GoService:
    """
    HTTP client service for the Go module proxy and index APIs.

    Encapsulates all network interactions required to enumerate Go packages,
    retrieve version lists, fetch version metadata, and download go.mod files
    for transitive dependency resolution. All responses are cached via
    CacheManager to reduce redundant network calls across the Dagster pipeline.

    External endpoints:
        - Index:  https://index.golang.org/index  (new module stream)
        - Proxy:  https://proxy.golang.org         (version info and go.mod)
    """

    def __init__(self) -> None:
        self.cache = get_cache_manager("go")
        self.PROXY_URL = "https://proxy.golang.org"
        self.INDEX_URL = "https://index.golang.org/index"
        self.orderer = get_orderer("GoPackage")
        self.MAX_ZIP_SIZE = 50 * 1024 * 1024  # 50 MB

    async def fetch_all_package_names(self, limit: int = 2000) -> list[str]:
        """
        Retrieve a deduplicated list of module paths from the Go module index.

        .. deprecated:: 1.2.0
            This method fetches a fixed-size batch from the start of the index
            and is not suitable for incremental loading. Use
            `fetch_packages_since` instead for reliable, cursor-based ingestion.

        The index endpoint returns newline-delimited JSON (NDJSON), where each
        line is a JSON object with at minimum a 'Path' field containing the
        module path. Results are cached for one hour to avoid hammering the
        index on repeated pipeline runs.

        Args:
            limit: Maximum number of index entries to fetch in a single request.
                   Defaults to 2000 as a reasonable batch size for incremental
                   ingestion.

        Returns:
            A deduplicated list of module path strings. Returns an empty list
            on any network or parse failure.
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
                        # The index returns NDJSON; parse each line as a full
                        # JSON object rather than using regex to avoid brittle
                        # string matching against potentially escaped characters.
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
        """
        Fetches a batch of module updates from the Go index since a given time.

        Uses the ?since=<timestamp> query parameter to retrieve a chronological
        stream of module publications. This enables incremental, cursor-based
        ingestion that is resumable and avoids reprocessing the entire index.

        Args:
            since: An RFC3339 timestamp string (e.g. "2019-04-10T19:08:52Z").
            limit: Max number of entries to return. Defaults to 2000.

        Returns:
            A tuple containing:
            - A list of unique module path strings from the batch.
            - The timestamp of the last entry, to be used as the cursor for
              the next iteration. Returns an empty string if the batch is empty.
        """
        url = f"{self.INDEX_URL}?since={since}&limit={limit}"
        session_manager = get_session_manager()
        session = await session_manager.get_session()

        try:
            async with session.get(url) as resp:
                if resp.status != 200:
                    return [], ""

                text = await resp.text()
                package_names: set[str] = set()
                last_timestamp = ""
                lines = text.splitlines()
                for line in lines:
                    if not line:
                        continue
                    try:
                        entry = loads(line)
                        if "Path" in entry:
                            package_names.add(entry["Path"])
                        if "Timestamp" in entry:
                            last_timestamp = entry["Timestamp"]
                    except (JSONDecodeError, KeyError):
                        continue

                return list(package_names), last_timestamp

        except ClientConnectorError as e:
            logger.error(f"GoService - Connection error with Index: {e}")
            return [], ""
        except Exception as e:
            logger.error(f"GoService - Unexpected error in fetch_packages_since: {e}")
            return [], ""

    async def fetch_versions_list(self, package_name: str) -> list[str]:
        """
        Fetch the list of tagged versions available for a module from the Go
        proxy /@v/list endpoint.

        The proxy returns a plain-text newline-separated list of version
        strings. Pseudo-versions and pre-release tags are included as-is.
        Results are cached for ten minutes given that new versions are
        published infrequently relative to the ingestion cycle.

        A 404 or 410 response indicates the module is not available via the
        proxy (e.g. it has been retracted or never published a tagged release)
        and is treated as an empty version list rather than an error.

        Args:
            package_name: Canonical module path (e.g. 'github.com/gin-gonic/gin').

        Returns:
            A list of version strings (e.g. ['v1.9.0', 'v1.8.2']). Returns an
            empty list if the module is not found or on repeated network failure
            after three retry attempts.
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
        """
        Fetch version metadata from the Go proxy /@v/{version}.info endpoint.

        The .info endpoint returns a JSON object containing at minimum 'Version'
        and 'Time' fields. The 'Time' field carries the canonical release
        timestamp for the version and is used to populate release_date in the
        graph. Results are cached for one hour.

        Args:
            package_name: Canonical module path.
            version:      Version string, with or without the leading 'v' prefix.

        Returns:
            A dictionary with the proxy response fields, or an empty dict if
            the version is not found (404/410) or on network failure.
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
        """
        Retrieves an ordered list of version descriptors for a module, filtering
        out Go pseudo-versions when tagged releases are available.

        Fetches raw version strings from the proxy and categorises them into
        tagged releases and pseudo-versions. If any tagged releases exist, only
        those are processed and returned. If a module has no tagged releases,
        this method falls back to processing the pseudo-versions to ensure the
        module is not omitted from the graph.

        The final list is delegated to an Orderer instance to be sorted by
        semantic version and assigned a serial number.

        Args:
            package_name: Canonical module path.

        Returns:
            A list of version descriptor dicts with at least 'name',
            'release_date', and 'serial_number' keys, ordered by ascending
            semantic version. Returns an empty list if no versions are available.
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

        versions_to_process = tagged_versions if tagged_versions else pseudo_versions

        formatted_versions = [
            {"name": v_str, "release_date": None} for v_str in versions_to_process
        ]
        return self.orderer.order_versions(formatted_versions)

    @staticmethod
    def _is_pseudo_version(version_str: str) -> bool:
        """
        Determines if a version string is a Go pseudo-version.

        Go pseudo-versions are generated for commits that do not have a version
        tag. They follow a strict format: vX.Y.Z-yyyymmddhhmmss-commit.
        This method uses a regex to distinguish them from standard pre-release
        tags (e.g., v1.0.0-beta.1), which should not be filtered.

        Args:
            version_str: The version string to check.

        Returns:
            True if the string matches the pseudo-version pattern, False otherwise.
        """
        # A pseudo-version is vX.Y.Z-timestamp-hash. Example: v0.0.0-20210101000000-abcdef123456
        pseudo_version_pattern = r"^v\d+\.\d+\.\d+-\d{14}-[a-f0-9]{12}$"
        return re.match(pseudo_version_pattern, version_str) is not None

    def get_repo_url(self, package_name: str) -> str:
        """
        Derive the source repository URL for a module from its path.

        For modules hosted on well-known VCS platforms (github.com, gitlab.com)
        the repository URL is constructed directly from the module path. For
        all other modules the canonical pkg.go.dev documentation URL is used
        as a fallback, which is publicly accessible and always valid for any
        module served through the Go proxy.

        Args:
            package_name: Canonical module path.

        Returns:
            A fully qualified HTTPS URL string.
        """
        if package_name.startswith("github.com") or package_name.startswith(
            "gitlab.com"
        ):
            return f"https://{package_name}"
        return f"https://pkg.go.dev/{package_name}"

    async def get_import_names(self, module_path: str, version: str) -> list[str]:
        """
        Extracts the list of importable package paths from a Go module's source.

        Downloads the module's .zip archive from the proxy, inspects it in
        memory, and identifies all directories containing at least one non-test
        .go file. The import path for each directory is constructed by joining
        the module path with the directory's relative path inside the zip.

        Args:
            module_path: The canonical module path.
            version: The version string of the module to inspect.

        Returns:
            A sorted list of unique import paths. Returns `[module_path]` as a
            safe fallback if the zip cannot be fetched or is invalid.
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
                    logger.warning(f"Skipping large module zip: {module_path}@{version}")
                    return [module_path]

            async with session.get(url) as resp:
                if resp.status != 200:
                    return [module_path]

                zip_bytes = await resp.read()
                zip_file = zipfile.ZipFile(io.BytesIO(zip_bytes))
                
                importable_dirs = set()
                prefix = f"{module_path}@{version}/"

                for name in zip_file.namelist():
                    if not name.startswith(prefix) or not name.endswith(".go") or name.endswith("_test.go"):
                        continue
                    
                    dir_name = "/".join(name[len(prefix):].split("/")[:-1])
                    importable_dirs.add(dir_name)
                
                import_names = {module_path}
                for dir_path in importable_dirs:
                    if dir_path:
                        import_names.add(f"{module_path}/{dir_path}")

                result = sorted(list(import_names))
                await self.cache.set_cache(cache_key, result, ttl=604800)  # 7 days
                return result

        except (ClientConnectorError, zipfile.BadZipFile, Exception) as e:
            logger.error(f"Failed to get import names for {module_path}@{version}: {e}")
            return [module_path]

    async def get_package_requirements(
        self, package_name: str, version: str
    ) -> dict[str, str]:
        """
        Download and parse the go.mod file for a specific module version from
        the Go proxy.

        The /@v/{version}.mod endpoint returns the raw go.mod content, which
        is parsed to extract the direct and indirect require directives. This
        is used during transitive dependency extraction to populate child
        package nodes in the graph.

        Args:
            package_name: Canonical module path of the parent module.
            version:      Tagged version string for which to fetch the go.mod.

        Returns:
            A dictionary mapping required module paths to their version strings.
            Returns an empty dict if the file cannot be fetched or parsed.
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
        except Exception as e:
            logger.error(
                f"GoService - Error fetching go.mod for {package_name}@{version}: {e}"
            )

        return {}

    def _parse_go_mod(self, content: str) -> dict[str, str]:
        """
        Parse require directives from raw go.mod content.

        Handles both block and single-line require forms. Inline comments
        (e.g. '// indirect') are stripped to prevent annotation tokens from
        being captured as part of the version string.

        Args:
            content: Raw text content of a go.mod file.

        Returns:
            A mapping of module paths to their declared version strings.
        """
        dependencies: dict[str, str] = {}

        # Block form: require ( ... )
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

        # Single-line form: require github.com/foo v1.0
        single_pattern = r"^require\s+(\S+)\s+(\S+)"
        for match in re.finditer(single_pattern, content, re.MULTILINE):
            dependencies[match.group(1)] = match.group(2)

        return dependencies
