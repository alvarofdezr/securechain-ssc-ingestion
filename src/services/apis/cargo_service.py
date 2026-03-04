from asyncio import sleep, to_thread
from io import BytesIO
from json import JSONDecodeError, loads
from pathlib import Path
from shutil import rmtree
from subprocess import run as subprocess_run
from tarfile import ReadError
from tarfile import open as open_tarfile
from typing import Any

from aiohttp import ClientConnectorError, ContentTypeError
from regex import MULTILINE, findall

from src.dependencies import (
    get_cache_manager,
    get_default_timeout,
    get_orderer,
    get_repo_normalizer,
    get_session_manager,
)
from src.logger import logger


class CargoService:
    def __init__(self):
        self.cache = get_cache_manager("cargo")
        self.BASE_URL = "https://crates.io/api/v1/crates"
        self.orderer = get_orderer("CargoPackage")
        self.repo_normalizer = get_repo_normalizer()

    async def fetch_all_package_names(self) -> list[str]:
        cached = await self.cache.get_cache("all_cargo_packages")
        if cached:
            logger.info(f"Cargo - Returning {len(cached)} cached package names")
            return cached

        logger.info("Cargo - Fetching all package names from crates.io index...")

        try:
            all_package_names = await to_thread(self.fetch_from_git_index)

            if all_package_names:
                logger.info(
                    f"Cargo - Successfully fetched {len(all_package_names)} crates from git index"
                )
                await self.cache.set_cache(
                    "all_cargo_packages", all_package_names, ttl=3600
                )
                return all_package_names
            else:
                logger.error("Cargo - Git index fetch returned empty")
                return []

        except Exception as e:
            logger.error(f"Cargo - Error fetching from git index: {e}")
            return []

    def fetch_from_git_index(self) -> list[str]:
        import tempfile

        temp_dir = Path(tempfile.mkdtemp())
        index_path = temp_dir / "crates.io-index"

        try:
            logger.info(f"Cargo - Cloning crates.io index to {index_path}...")

            result = subprocess_run(
                [
                    "git",
                    "clone",
                    "--depth=1",
                    "https://github.com/rust-lang/crates.io-index.git",
                    str(index_path),
                ],
                capture_output=True,
                text=True,
                timeout=300,
            )

            if result.returncode != 0:
                logger.error(f"Cargo - Git clone failed: {result.stderr}")
                return []

            logger.info("Cargo - Git clone completed, extracting package names...")

            package_names = set()

            for json_file in index_path.rglob("*"):
                if (
                    json_file.is_file()
                    and not json_file.name.startswith(".")
                    and json_file.name != "config.json"
                ):
                    try:
                        with open(json_file, encoding="utf-8") as f:
                            for line in f:
                                line = line.strip()
                                if line:
                                    try:
                                        data = loads(line)
                                        name = data.get("name")
                                        if name:
                                            package_names.add(name)
                                    except Exception:
                                        continue
                    except Exception as e:
                        logger.warning(f"Cargo - Error reading {json_file}: {e}")
                        continue

            logger.info(
                f"Cargo - Extracted {len(package_names)} package names from git index"
            )
            return sorted(package_names)

        except Exception as e:
            logger.error(f"Cargo - Error in _fetch_from_git_index: {e}")
            return []
        finally:
            try:
                logger.info(f"Cargo - Cleaning up temp directory {temp_dir}...")
                rmtree(temp_dir, ignore_errors=True)
            except Exception as e:
                logger.warning(f"Cargo - Error cleaning up temp directory: {e}")

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

        url = f"{self.BASE_URL}/{package_name}/{version_name}/dependencies"
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
        for version_data in metadata.get("versions", []):
            version = version_data.get("num")
            created_at = version_data.get("created_at")
            if version:
                raw_versions.append({"name": version, "release_date": created_at})
        return raw_versions

    async def get_versions(self, metadata: dict[str, Any]) -> list[dict[str, Any]]:
        if not metadata:
            return []
        raw = self.extract_raw_versions(metadata)
        return self.orderer.order_versions(raw)

    def get_repo_url(self, metadata: dict[str, Any]) -> str | None:
        if not metadata:
            return None

        crate_data = metadata.get("crate", {})
        raw_url = crate_data.get("repository")

        if raw_url:
            norm_url = self.repo_normalizer.normalize(raw_url)
            if self.repo_normalizer.check():
                return norm_url

        homepage = crate_data.get("homepage")
        if homepage:
            norm_url = self.repo_normalizer.normalize(homepage)
            if self.repo_normalizer.check():
                return norm_url

        return None

    def get_package_requirements(self, metadata: dict[str, Any]) -> dict[str, Any]:
        requirements: dict[str, Any] = {}

        if not metadata:
            return requirements

        dependencies = metadata.get("dependencies", []) or []
        for dep in dependencies:
            crate_id = dep.get("crate_id")
            req = dep.get("req")
            if crate_id:
                requirements[crate_id.lower()] = req or ""

        return requirements

    async def extract_import_names(self, crate_name: str, version: str) -> list[str]:
        cache_key = f"import_names:{crate_name}:{version}"
        cached = await self.cache.get_cache(cache_key)
        if cached:
            return cached

        download_url = (
            f"https://crates.io/api/v1/crates/{crate_name}/{version}/download"
        )
        session_manager = get_session_manager()
        session = await session_manager.get_session()

        try:
            async with session.get(download_url, timeout=get_default_timeout()) as resp:
                if resp.status != 200:
                    logger.warning(
                        f"Cargo - Failed to download {crate_name}@{version}: HTTP {resp.status}"
                    )
                    return []

                crate_bytes = await resp.read()
                import_names = await to_thread(
                    self.extract_from_tarball, crate_name, crate_bytes
                )

                await self.cache.set_cache(cache_key, import_names, ttl=604800)
                return import_names

        except Exception as e:
            logger.error(
                f"Cargo - Error extracting import_names for {crate_name}@{version}: {e}"
            )
            return []

    def extract_from_tarball(self, crate_name: str, crate_bytes: bytes) -> list[str]:
        import_names = set()

        try:
            with open_tarfile(fileobj=BytesIO(crate_bytes), mode="r:gz") as tar:
                rs_files = [m for m in tar.getmembers() if m.name.endswith(".rs")]

                for member in rs_files:
                    try:
                        file_extracted = tar.extractfile(member)
                        if file_extracted is not None:
                            file_data = file_extracted.read().decode(
                                "utf-8", errors="replace"
                            )
                            import_names.update(
                                self.extract_public_items(crate_name, file_data)
                            )

                    except UnicodeDecodeError:
                        logger.warning(f"Cargo - Unicode decode error in {member.name}")
                        continue
                    except Exception as e:
                        logger.warning(f"Cargo - Error processing {member.name}: {e}")
                        continue

            import_names.add(crate_name)
            return sorted(import_names)

        except ReadError as e:
            logger.error(f"Cargo - Error reading tarball for {crate_name}: {e}")
            return []
        except Exception as e:
            logger.error(f"Cargo - Unexpected error extracting {crate_name}: {e}")
            return []

    def extract_public_items(self, crate_name: str, file_data: str) -> set[str]:
        items = set()

        pub_mods = findall(r"\bpub\s+mod\s+(\w+)", file_data)
        for mod in pub_mods:
            items.add(f"{crate_name}::{mod}")

        pub_uses = findall(r"\bpub\s+use\s+(?:[\w:]+::)?(\w+)", file_data)
        for name in pub_uses:
            items.add(f"{crate_name}::{name}")

        pub_structs = findall(r"\bpub\s+struct\s+(\w+)", file_data)
        for struct in pub_structs:
            items.add(f"{crate_name}::{struct}")

        pub_enums = findall(r"\bpub\s+enum\s+(\w+)", file_data)
        for enum in pub_enums:
            items.add(f"{crate_name}::{enum}")

        pub_traits = findall(r"\bpub\s+trait\s+(\w+)", file_data)
        for trait in pub_traits:
            items.add(f"{crate_name}::{trait}")

        pub_fns = findall(
            r"^\s*pub\s+(?:const\s+|async\s+|unsafe\s+)*fn\s+(\w+)",
            file_data,
            MULTILINE,
        )
        for fn in pub_fns:
            items.add(f"{crate_name}::{fn}")

        pub_consts = findall(r"\bpub\s+const\s+(\w+)", file_data)
        for const in pub_consts:
            items.add(f"{crate_name}::{const}")

        pub_statics = findall(r"\bpub\s+static\s+(\w+)", file_data)
        for static in pub_statics:
            items.add(f"{crate_name}::{static}")

        pub_macro_rules = findall(
            r"#\[macro_export\]\s*macro_rules!\s+(\w+)", file_data
        )
        for macro in pub_macro_rules:
            items.add(f"{crate_name}::{macro}")

        pub_macros = findall(r"\bpub\s+macro\s+(\w+)", file_data)
        for macro in pub_macros:
            items.add(f"{crate_name}::{macro}")

        pub_types = findall(r"\bpub\s+type\s+(\w+)", file_data)
        for type_alias in pub_types:
            items.add(f"{crate_name}::{type_alias}")

        return items
