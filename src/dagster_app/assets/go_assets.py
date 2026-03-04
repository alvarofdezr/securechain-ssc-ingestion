from asyncio import run
from typing import Any

from dagster import AssetExecutionContext, MetadataValue, Output, asset

from src.dependencies import (
    get_attributor,
    get_db,
    get_package_service,
    get_redis_queue,
    get_version_service,
)
from src.logger import logger
from src.processes.extractors import GoPackageExtractor
from src.processes.updaters import GoVersionUpdater
from src.schemas import GoPackageSchema
from src.services.apis.go_service import GoService


@asset(
    description="Ingests new Go packages from the Go Index/Proxy",
    group_name="go",
    compute_kind="python",
)
def go_package_ingestion(
    context: AssetExecutionContext,
) -> Output[dict[str, Any]]:
    """
    Dagster asset that performs incremental, cursor-based ingestion of Go modules.

    Fetches modules from the Go index (index.golang.org) published since the
    last successful run. The cursor (a timestamp) is stored in Redis under
    'go_ingestion_cursor' so it persists across container restarts and Dagster
    runs without relying on /tmp (which is ephemeral in Docker).

    On each run the asset:
    1. Reads the cursor from Redis (defaults to Go proxy launch date).
    2. Fetches a batch of new module paths from the index.
    3. Processes each path: skip if already in graph, otherwise extract.
    4. Advances the cursor after each successful batch.
    5. Repeats until the index is fully caught up (empty batch returned).

    The cursor only advances after a batch completes successfully, so a crash
    mid-batch will reprocess that batch on the next run (safe due to MERGE in
    the graph queries).
    """
    try:
        logger.info("Starting Go package ingestion process")
        go_svc = GoService()

        DEFAULT_CURSOR = "2019-04-10T19:08:52.997264Z"
        CURSOR_KEY = "go_ingestion_cursor"

        async def _run() -> dict[str, Any]:
            await get_db().initialize()

            package_svc = get_package_service()
            version_svc = get_version_service()
            attr = get_attributor()
            redis = get_redis_queue()

            new_packages = 0
            skipped_packages = 0
            error_count = 0
            total_scanned = 0

            start_cursor = redis.r.get(CURSOR_KEY) or DEFAULT_CURSOR
            current_cursor = start_cursor

            context.log.info(f"Go - Starting ingestion from cursor: {current_cursor}")

            while True:
                package_names, next_cursor = await go_svc.fetch_packages_since(
                    current_cursor
                )

                if not package_names or next_cursor == current_cursor:
                    context.log.info(
                        "Go - No new packages found. Index is fully caught up."
                    )
                    break

                batch_size = len(package_names)
                total_scanned += batch_size
                context.log.info(
                    f"Go - Processing batch of {batch_size} packages "
                    f"(cursor: {current_cursor})"
                )

                for package_name in package_names:
                    try:
                        existing = await package_svc.read_package_by_name(
                            "GoPackage", package_name
                        )
                        if existing:
                            skipped_packages += 1
                            continue

                        package_schema = GoPackageSchema(name=package_name)
                        extractor = GoPackageExtractor(
                            package=package_schema,
                            package_service=package_svc,
                            version_service=version_svc,
                            go_service=go_svc,
                            attributor=attr,
                        )
                        await extractor.run()
                        new_packages += 1
                        context.log.info(
                            f"Go - Ingested: {package_name} "
                            f"(new: {new_packages}, skipped: {skipped_packages})"
                        )

                    except Exception as e:
                        error_count += 1
                        logger.error(f"Go - Error ingesting {package_name}: {e}")
                        context.log.error(f"Go - Error ingesting {package_name}: {e}")

                current_cursor = next_cursor
                redis.r.set(CURSOR_KEY, current_cursor)
                context.log.info(f"Go - Cursor advanced to: {current_cursor}")

            logger.info(
                f"Go ingestion completed. "
                f"Scanned: {total_scanned}, New: {new_packages}, "
                f"Skipped: {skipped_packages}, Errors: {error_count}"
            )

            return {
                "total_scanned": total_scanned,
                "new_packages_ingested": new_packages,
                "skipped_existing": skipped_packages,
                "errors": error_count,
                "cursor_start": start_cursor,
                "cursor_end": current_cursor,
            }

        stats = run(_run())

        return Output(
            value=stats,
            metadata={
                "total_scanned": stats["total_scanned"],
                "new_packages_ingested": stats["new_packages_ingested"],
                "skipped_existing": stats["skipped_existing"],
                "errors": stats["errors"],
                "cursor_start": MetadataValue.text(stats["cursor_start"]),
                "cursor_end": MetadataValue.text(stats["cursor_end"]),
            },
        )

    except Exception as e:
        logger.error(f"Go - Fatal error in ingestion process: {e}")
        raise


@asset(
    description="Updates Go package versions in SecureChain graph",
    group_name="go",
    compute_kind="python",
)
def go_packages_updates(
    context: AssetExecutionContext,
) -> Output[dict[str, Any]]:
    """
    Dagster asset that incrementally updates Go module versions in the graph.

    Iterates over all GoPackage nodes in the graph in batches and delegates
    version synchronisation to GoVersionUpdater. Only versions absent from the
    graph are attributed and persisted.
    """
    try:
        logger.info("Starting Go package version update process")
        go_svc = GoService()

        async def _run() -> dict[str, Any]:
            await get_db().initialize()

            package_svc = get_package_service()
            version_svc = get_version_service()
            attr = get_attributor()

            updater = GoVersionUpdater(go_svc, package_svc, version_svc, attr)

            package_count = 0
            version_count = 0
            error_count = 0

            async for batch in package_svc.read_packages_in_batches(
                "GoPackage", batch_size=100
            ):
                for pkg in batch:
                    try:
                        await updater.update_package_versions(pkg)
                        package_count += 1

                        versions = await version_svc.count_number_of_versions_by_package(
                            "GoPackage", pkg["name"]
                        )
                        version_count += versions
                        context.log.info(
                            f"Go - Updated {pkg['name']} "
                            f"(total processed: {package_count})"
                        )
                    except Exception as e:
                        error_count += 1
                        logger.error(f"Go - Error updating {pkg['name']}: {e}")

            logger.info(
                f"Go update completed. Packages: {package_count}, Errors: {error_count}"
            )

            return {
                "packages_processed": package_count,
                "total_versions": version_count,
                "errors": error_count,
            }

        stats = run(_run())

        return Output(
            value=stats,
            metadata={
                "packages_processed": stats["packages_processed"],
                "total_versions": stats["total_versions"],
                "errors": stats["errors"],
                "success_rate": MetadataValue.float(
                    (
                        stats["packages_processed"]
                        / (stats["packages_processed"] + stats["errors"])
                        * 100
                    )
                    if (stats["packages_processed"] + stats["errors"]) > 0
                    else 0.0
                ),
            },
        )

    except Exception as e:
        logger.error(f"Go - Fatal error in update process: {e}")
        raise