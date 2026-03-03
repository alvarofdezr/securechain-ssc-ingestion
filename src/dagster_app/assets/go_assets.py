from asyncio import run
from typing import Any

from dagster import AssetExecutionContext, MetadataValue, Output, asset

from src.dependencies import (
    get_attributor,
    get_db,
    get_package_service,
    get_redis_queue,
    get_version_service,
    get_cache_manager,
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

    This asset fetches modules from the Go index (index.golang.org) that have
    been published since the last successful run. It uses a timestamp (cursor)
    stored in Redis to track its position in the index's chronological feed.

    On each run, it fetches the current cursor, queries the index for new
    packages, and processes them in batches. If a batch is successful, the
    cursor is updated to the timestamp of the last processed entry, making the
    ingestion process resumable and avoiding redundant work. If no cursor is
    found, it defaults to the Go proxy's launch date.

    Returns:
        An Output wrapping a stats dictionary and Dagster metadata for UI
        observability, including the starting and ending cursor values.
    """
    try:
        logger.info("Starting Go package ingestion process")
        go_svc = GoService()
        redis = get_redis_queue()
        cursor_key = "go_ingestion_cursor"
        # The Go module proxy was launched on this date.
        default_cursor = "2019-04-10T19:08:52.997264Z"

        async def _run() -> dict[str, Any]:
            await get_db().initialize()

            package_svc = get_package_service()
            version_svc = get_version_service()
            attr = get_attributor()

            new_packages = 0
            skipped_packages = 0
            error_count = 0
            total_packages = 0

            start_cursor = await redis.get(cursor_key) or default_cursor
            current_cursor = start_cursor
            context.log.info(f"Go - Starting ingestion from cursor: {current_cursor}")

            while True:
                package_names, next_cursor = await go_svc.fetch_packages_since(
                    current_cursor
                )
                if not package_names:
                    context.log.info("Go - No new packages found. Ingestion caught up.")
                    break

                batch_size = len(package_names)
                total_packages += batch_size
                context.log.info(f"Go - Processing batch of {batch_size} packages.")

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

                    except Exception as e:
                        error_count += 1
                        logger.error(f"Go - Error ingesting {package_name}: {e}")
                        context.log.error(f"Go - Error ingesting {package_name}: {e}")

                current_cursor = next_cursor
                await redis.set(cursor_key, current_cursor)
                context.log.info(f"Go - Batch processed. New cursor: {current_cursor}")

            return {
                "total_in_index_batch": total_packages,
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
                "total_scanned": stats["total_in_index_batch"],
                "new_packages_ingested": stats["new_packages_ingested"],
                "cursor_start": stats["cursor_start"],
                "cursor_end": stats["cursor_end"],
                "errors": stats["errors"],
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

    Iterates over all GoPackage nodes currently stored in the graph in batches
    and delegates version synchronisation to GoVersionUpdater. Only new
    versions discovered since the last run are attributed and persisted, making
    this asset efficient for frequent scheduling.

    Returns:
        An Output wrapping a stats dictionary with the following keys:
          - packages_processed: Total number of packages evaluated.
          - total_versions:     Cumulative version count across all packages.
          - errors:             Count of packages that failed during update.
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
                            f"(Total processed: {package_count})"
                        )
                    except Exception as e:
                        error_count += 1
                        logger.error(f"Go - Error updating {pkg['name']}: {e}")

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
                "errors": stats["errors"],
            },
        )

    except Exception as e:
        logger.error(f"Go - Fatal error in update process: {e}")
        raise
