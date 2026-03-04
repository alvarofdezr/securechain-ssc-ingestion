from asyncio import sleep, to_thread
from io import BytesIO
from json import JSONDecodeError, loads
from pathlib import Path
from subprocess import TimeoutExpired, run
from typing import Any
from zipfile import BadZipFile, ZipFile

from aiohttp import ClientConnectorError, ContentTypeError

from src.dependencies import (
    get_cache_manager,
    get_default_timeout,
    get_orderer,
    get_repo_normalizer,
    get_session_manager,
)
from src.logger import logger


class MavenService:
    def __init__(self):
        self.cache = get_cache_manager("maven")
        self.BASE_URL = "https://search.maven.org/solrsearch/select"
        self.CENTRAL_URL = "https://repo1.maven.org/maven2"
        self.orderer = get_orderer("MavenPackage")
        self.repo_normalizer = get_repo_normalizer()

    async def fetch_all_package_names(self) -> list[str]:
        cached = await self.cache.get_cache("all_mvn_packages")
        if cached:
            logger.info(f"Maven - Using cached package list ({len(cached):,} packages)")
            return cached

        maven_utils_dir = Path(__file__).parent.parent.parent / "utils" / "maven"

        logger.info("Maven - Generating package list using Lucene index extraction...")
        try:
            all_packages = await to_thread(self.run_maven_extraction, maven_utils_dir)
            if all_packages:
                logger.info(
                    f"Maven - Extracted {len(all_packages):,} packages from Maven Central"
                )
                await self.cache.set_cache("all_mvn_packages", all_packages, ttl=3600)
                return all_packages
            else:
                logger.warning("Maven - Extraction returned empty list")
        except Exception as e:
            logger.error(f"Maven - Error running extraction: {e}")

        return []

    def run_maven_extraction(self, maven_utils_dir: Path) -> list[str]:
        try:
            dockerfile = maven_utils_dir / "Dockerfile.maven"
            if not dockerfile.exists():
                logger.error(f"Maven - Dockerfile not found at {dockerfile}")
                return []

            docker_image = "maven-extractor"

            logger.info("Maven - Building Docker image...")
            build_cmd = [
                "docker",
                "build",
                "-t",
                docker_image,
                "-f",
                str(dockerfile),
                str(maven_utils_dir),
            ]

            build_result = run(build_cmd, capture_output=True, text=True, timeout=600)

            if build_result.returncode != 0:
                logger.error(f"Maven - Docker build failed: {build_result.stderr}")
                return []

            logger.info(
                "Maven - Running extraction container (this may take 1-2 hours)..."
            )

            run_cmd = ["docker", "run", "--rm", docker_image]

            run_result = run(run_cmd, capture_output=True, text=True, timeout=10800)

            if run_result.returncode != 0:
                logger.error(f"Maven - Extraction failed: {run_result.stderr}")
                return []

            if run_result.stderr:
                for line in run_result.stderr.strip().split("\n")[-5:]:
                    if line:
                        logger.info(f"Maven - {line}")

            try:
                all_packages = loads(run_result.stdout)
                logger.info(
                    f"Maven - Successfully parsed {len(all_packages):,} packages"
                )
                return all_packages
            except JSONDecodeError as e:
                logger.error(f"Maven - Failed to parse JSON output: {e}")
                return []

        except TimeoutExpired:
            logger.error("Maven - Extraction timeout (exceeded 2 hours)")
            return []
        except Exception as e:
            logger.error(f"Maven - Unexpected error: {e}")
            return []

    async def fetch_package_metadata(
        self, group_id: str, artifact_id: str
    ) -> dict[str, Any]:
        package_name = f"{group_id}:{artifact_id}"
        cached = await self.cache.get_cache(package_name)
        if cached:
            return cached

        url = f"{self.BASE_URL}?q=g:{group_id}+AND+a:{artifact_id}&core=gav&rows=200&wt=json"
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
        self, group_id: str, artifact_id: str, version_name: str
    ) -> dict[str, Any]:
        cache_key = f"{group_id}:{artifact_id}:{version_name}"
        cached = await self.cache.get_cache(cache_key)
        if cached:
            return cached

        group_path = group_id.replace(".", "/")
        url = f"{self.CENTRAL_URL}/{group_path}/{artifact_id}/{version_name}/{artifact_id}-{version_name}.pom"
        session_manager = get_session_manager()
        session = await session_manager.get_session()

        for _ in range(3):
            try:
                async with session.get(url) as resp:
                    pom_content = await resp.text()
                    response = {"pom": pom_content}
                    await self.cache.set_cache(cache_key, response, ttl=600)
                    return response
            except (ClientConnectorError, TimeoutError):
                await sleep(5)
            except (JSONDecodeError, ContentTypeError):
                return {}
        return {}

    def extract_raw_versions(self, metadata: dict[str, Any]) -> list[dict[str, Any]]:
        raw_versions = []
        response = metadata.get("response", {})
        docs = response.get("docs", [])

        for doc in docs:
            version = doc.get("v")
            timestamp = doc.get("timestamp")
            if version:
                raw_versions.append({"name": version, "release_date": timestamp})
        return raw_versions

    async def get_versions(self, metadata: dict[str, Any]) -> list[dict[str, Any]]:
        if not metadata:
            return []
        raw = self.extract_raw_versions(metadata)
        return self.orderer.order_versions(raw)

    async def get_repo_url(self, metadata: dict[str, Any]) -> str | None:
        if not metadata:
            return None

        response = metadata.get("response", {})
        docs = response.get("docs", [])

        if docs:
            latest_doc = docs[0]
            group_id = latest_doc.get("g")
            artifact_id = latest_doc.get("a")
            version = latest_doc.get("v")

            if group_id and artifact_id and version:
                pom_data = await self.fetch_package_version_metadata(
                    group_id, artifact_id, version
                )
                if pom_data and pom_data.get("pom"):
                    pom_content = pom_data["pom"]
                    if "<scm>" in pom_content and "<url>" in pom_content:
                        try:
                            scm_start = pom_content.find("<scm>")
                            scm_end = pom_content.find("</scm>", scm_start)
                            scm_section = pom_content[scm_start:scm_end]

                            url_start = scm_section.find("<url>") + 5
                            url_end = scm_section.find("</url>", url_start)
                            raw_url = scm_section[url_start:url_end].strip()

                            if raw_url:
                                norm_url = self.repo_normalizer.normalize(raw_url)
                                if self.repo_normalizer.check():
                                    return norm_url
                        except Exception:
                            pass

        return None

    def get_package_requirements(self, metadata: dict[str, Any]) -> dict[str, Any]:
        requirements: dict[str, Any] = {}

        if not metadata or not metadata.get("pom"):
            return requirements

        pom_content = metadata["pom"]

        if "<dependencies>" in pom_content:
            try:
                deps_start = pom_content.find("<dependencies>")
                deps_end = pom_content.find("</dependencies>", deps_start)
                deps_section = pom_content[deps_start:deps_end]

                dependency_blocks = []
                start = 0
                while True:
                    dep_start = deps_section.find("<dependency>", start)
                    if dep_start == -1:
                        break
                    dep_end = deps_section.find("</dependency>", dep_start)
                    if dep_end == -1:
                        break
                    dependency_blocks.append(deps_section[dep_start : dep_end + 13])
                    start = dep_end + 13

                for dep_block in dependency_blocks:
                    try:
                        group_start = dep_block.find("<groupId>") + 9
                        group_end = dep_block.find("</groupId>", group_start)
                        dep_group = dep_block[group_start:group_end].strip()

                        artifact_start = dep_block.find("<artifactId>") + 12
                        artifact_end = dep_block.find("</artifactId>", artifact_start)
                        dep_artifact = dep_block[artifact_start:artifact_end].strip()

                        version_start = dep_block.find("<version>")
                        if version_start != -1:
                            version_start += 9
                            version_end = dep_block.find("</version>", version_start)
                            dep_version = dep_block[version_start:version_end].strip()
                        else:
                            dep_version = ""

                        if dep_group and dep_artifact:
                            requirements[f"{dep_group}:{dep_artifact}".lower()] = (
                                dep_version
                            )
                    except Exception:
                        pass
            except Exception:
                pass

        return requirements

    async def extract_import_names(
        self, group_id: str, artifact_id: str, version: str
    ) -> list[str]:
        cache_key = f"import_names:{group_id}:{artifact_id}:{version}"
        cached = await self.cache.get_cache(cache_key)
        if cached:
            return cached

        group_path = group_id.replace(".", "/")
        download_url = f"{self.CENTRAL_URL}/{group_path}/{artifact_id}/{version}/{artifact_id}-{version}.jar"
        session_manager = get_session_manager()
        session = await session_manager.get_session()

        try:
            async with session.get(download_url, timeout=get_default_timeout()) as resp:
                if resp.status != 200:
                    logger.warning(
                        f"Maven - Failed to download {group_id}:{artifact_id}:{version}: HTTP {resp.status}"
                    )
                    return []

                jar_bytes = await resp.read()
                import_names = await to_thread(self.extract_from_jar, jar_bytes)

                await self.cache.set_cache(cache_key, import_names, ttl=604800)
                return import_names

        except Exception as e:
            logger.error(
                f"Maven - Error extracting import_names for {group_id}:{artifact_id}:{version}: {e}"
            )
            return []

    def extract_from_jar(self, jar_bytes: bytes) -> list[str]:
        try:
            with ZipFile(BytesIO(jar_bytes)) as jar:
                all_packages = set()
                for entry in jar.namelist():
                    if (
                        entry.endswith(".class")
                        and not entry.startswith("META-INF")
                        and "$" not in entry
                    ):
                        parts = entry.split("/")
                        if len(parts) > 1:
                            package = ".".join(parts[:-1])
                            all_packages.add(package)

            sorted_packages: list[str] = sorted(
                all_packages, key=lambda p: len(p.split("."))
            )

            general_imports: list[str] = []
            for pkg in sorted_packages:
                if not any(pkg.startswith(parent + ".") for parent in general_imports):
                    general_imports.append(pkg)

            return general_imports

        except BadZipFile:
            logger.warning("Maven - Bad JAR file (corrupted)")
            return []
        except Exception as e:
            logger.error(f"Maven - Unexpected error extracting JAR: {e}")
            return []
