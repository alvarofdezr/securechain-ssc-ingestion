#!/usr/bin/env python3
from json import dumps
from pathlib import Path
from subprocess import CalledProcessError, run
from sys import exit, stderr
from urllib.request import urlretrieve

from lucene import initVM
from lupyne import engine

INDEX_URL = "https://repo1.maven.org/maven2/.index/nexus-maven-repository-index.gz"
CLI_JAR_URL = "https://repo1.maven.org/maven2/org/apache/maven/indexer/indexer-cli/6.2.0/indexer-cli-6.2.0.jar"
CLI_JAR = Path("indexer-cli-6.2.0.jar")
INDEX_GZ = Path("nexus-maven-repository-index.gz")
EXPANDED_DIR = Path("nexus-index-expanded")
OUTPUT_FILE = Path("all_maven_packages.txt")


def log(msg):
    print(msg, flush=True, file=stderr)


def download_file(url, dest, force=False):
    if dest.exists() and not force:
        log(f"File {dest.name} already exists, skipping download.")
        return

    if dest.exists():
        log(f"Removing old {dest.name}...")
        dest.unlink()

    log(f"Downloading {dest.name}...")
    try:
        urlretrieve(url, dest)
        size_mb = dest.stat().st_size / 1e6
        log(f"Downloaded {dest.name} ({size_mb:.1f} MB)")
    except Exception as e:
        log(f"Error downloading {dest.name}: {e}")
        raise


def expand_index():
    import shutil

    if EXPANDED_DIR.exists():
        log(f"Removing old expanded index at {EXPANDED_DIR}...")
        shutil.rmtree(EXPANDED_DIR)

    EXPANDED_DIR.mkdir(parents=True, exist_ok=True)

    log("Expanding Lucene index...")
    cmd = ["java", "-jar", str(CLI_JAR), "-u", str(INDEX_GZ), "-d", str(EXPANDED_DIR)]

    try:
        run(cmd, check=True, capture_output=True, text=True)
    except CalledProcessError as e:
        log(f"Error expanding index: {e.stderr}")
        raise


def extract_packages():
    log("Initializing JVM (PyLucene)...")
    try:
        initVM(vmargs=["-Djava.awt.headless=true"])
    except Exception as e:
        log(f"Error initializing JVM: {e}")
        raise

    log(f"Opening Lucene index from: {EXPANDED_DIR.resolve()}")
    try:
        index = engine.IndexSearcher(str(EXPANDED_DIR))
    except Exception as e:
        log(f"Error opening index: {e}")
        raise

    packages = set()
    max_docs = index.maxDoc()
    log(f"Extracting from {max_docs:,} documents...")

    for i in range(max_docs):
        try:
            doc = index.doc(i)
            u = doc.get("u")
            if u and "|" in u:
                parts = u.split("|")
                if len(parts) >= 2:
                    g, a = parts[0], parts[1]
                    packages.add(f"{g}:{a}")

            if i > 0 and i % 100000 == 0:
                progress = (i / max_docs) * 100
                log(
                    f"Progress: {i:,}/{max_docs:,} docs ({progress:.1f}%) - {len(packages):,} unique packages"
                )
        except Exception:
            continue

    log(f"Extraction complete: {len(packages):,} unique packages found")

    sorted_packages = sorted(packages)

    log(f"Writing {len(sorted_packages):,} packages to stdout as JSON...")
    print(dumps(sorted_packages), flush=True)

    return len(sorted_packages)


def main():
    try:
        log("Starting Maven package extraction with latest index...")
        download_file(CLI_JAR_URL, CLI_JAR, force=False)
        download_file(INDEX_URL, INDEX_GZ, force=True)
        expand_index()

        package_count = extract_packages()

        log("Cleaning up temporary download files...")
        for f in [CLI_JAR, INDEX_GZ]:
            if f.exists():
                try:
                    f.unlink()
                    log(f"Removed {f.name}")
                except Exception as e:
                    log(f"Warning: Could not remove {f.name}: {e}")

        log(f"Result: {package_count:,} Maven packages extracted")
        return 0

    except Exception as e:
        log(f"Fatal error: {e}")
        print(dumps([]), flush=True)
        return 1


if __name__ == "__main__":
    exit(main())
