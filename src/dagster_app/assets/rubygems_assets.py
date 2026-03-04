from asyncio import run
from typing import Any

from dagster import AssetExecutionContext, MetadataValue, Output, asset

from src.dependencies import (
    get_attributor,
    get_db,
    get_package_service,
    get_rubygems_service,
    get_version_service,
)
from src.logger import logger
from src.processes.extractors import RubyGemsPackageExtractor
from src.processes.updaters import RubyGemsVersionUpdater
from src.schemas import RubyGemsPackageSchema


@asset(
    description="Ingests new RubyGems from rubygems.org",
    group_name="rubygems",
    compute_kind="python",
)
def rubygems_package_ingestion(
    context: AssetExecutionContext,
) -> Output[dict[str, Any]]:
    try:
        logger.info("Starting RubyGems package ingestion process")

        rubygems_svc = get_rubygems_service()

        async def _run():
            await get_db().initialize()

            package_svc = get_package_service()
            version_svc = get_version_service()
            attr = get_attributor()

            new_packages = 0
            skipped_packages = 0
            error_count = 0

            all_gem_names = await rubygems_svc.fetch_all_package_names()
            total_gems = len(all_gem_names)

            logger.info(f"RubyGems - Found {total_gems} gems in rubygems.org")
            context.log.info(f"RubyGems - Found {total_gems} gems in rubygems.org")

            for idx, gem_name in enumerate(all_gem_names, 1):
                try:
                    gem_name_lower = gem_name.lower()

                    existing_package = await package_svc.read_package_by_name(
                        "RubyGemsPackage", gem_name_lower
                    )

                    if existing_package:
                        skipped_packages += 1
                        if idx % 1000 == 0:
                            context.log.info(
                                f"RubyGems - Progress: {idx}/{total_gems} (New: {new_packages}, Skipped: {skipped_packages})"
                            )
                        continue

                    package_schema = RubyGemsPackageSchema(name=gem_name_lower)

                    extractor = RubyGemsPackageExtractor(
                        package=package_schema,
                        package_service=package_svc,
                        version_service=version_svc,
                        rubygems_service=rubygems_svc,
                        attributor=attr,
                    )

                    await extractor.run()
                    new_packages += 1

                    context.log.info(
                        f"RubyGems - Ingested new gem: {gem_name_lower} ({new_packages} new gems)"
                    )

                    if idx % 100 == 0:
                        context.log.info(
                            f"RubyGems - Progress: {idx}/{total_gems} (New: {new_packages}, Skipped: {skipped_packages})"
                        )

                except Exception as e:
                    error_count += 1
                    logger.error(f"RubyGems - Error ingesting {gem_name}: {e}")
                    context.log.error(f"RubyGems - Error ingesting {gem_name}: {e}")

            logger.info(
                f"RubyGems ingestion process completed. New gems: {new_packages}, Skipped: {skipped_packages}, Errors: {error_count}"
            )

            return {
                "total_in_registry": total_gems,
                "new_packages_ingested": new_packages,
                "skipped_existing": skipped_packages,
                "errors": error_count,
            }

        stats = run(_run())

        return Output(
            value=stats,
            metadata={
                "total_in_registry": stats["total_in_registry"],
                "new_packages_ingested": stats["new_packages_ingested"],
                "skipped_existing": stats["skipped_existing"],
                "errors": stats["errors"],
                "ingestion_rate": MetadataValue.float(
                    (stats["new_packages_ingested"] / stats["total_in_registry"] * 100)
                    if stats["total_in_registry"] > 0
                    else 0.0
                ),
            },
        )

    except Exception as e:
        logger.error(f"RubyGems - Fatal error in ingestion process: {e}")
        raise


@asset(
    description="Updates RubyGems package versions in SecureChain graph",
    group_name="rubygems",
    compute_kind="python",
)
def rubygems_packages_updates(
    context: AssetExecutionContext,
) -> Output[dict[str, Any]]:
    try:
        logger.info("Starting RubyGems package version update process")

        rubygems_svc = get_rubygems_service()

        async def _run():
            await get_db().initialize()

            package_svc = get_package_service()
            version_svc = get_version_service()
            attr = get_attributor()

            updater = RubyGemsVersionUpdater(
                rubygems_svc, package_svc, version_svc, attr
            )

            package_count = 0
            version_count = 0
            error_count = 0

            async for batch in package_svc.read_packages_in_batches(
                "RubyGemsPackage", batch_size=100
            ):
                for pkg in batch:
                    try:
                        await updater.update_package_versions(pkg)
                        package_count += 1

                        versions = (
                            await version_svc.count_number_of_versions_by_package(
                                "RubyGemsPackage", pkg["name"]
                            )
                        )
                        version_count += versions

                        context.log.info(
                            f"RubyGems - Successfully updated {pkg['name']} (Total: {package_count})"
                        )
                    except Exception as e:
                        error_count += 1
                        logger.error(f"RubyGems - Error updating {pkg['name']}: {e}")

            logger.info(
                f"RubyGems update process completed. Total packages: {package_count}"
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
        logger.error(f"RubyGems - Fatal error in update process: {e}")
        raise
