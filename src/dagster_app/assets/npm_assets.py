from asyncio import run
from typing import Any

from dagster import AssetExecutionContext, MetadataValue, Output, asset

from src.dependencies import (
    get_attributor,
    get_db,
    get_npm_service,
    get_package_service,
    get_version_service,
)
from src.logger import logger
from src.processes.extractors import NPMPackageExtractor
from src.processes.updaters import NPMVersionUpdater
from src.schemas import NPMPackageSchema


@asset(
    description="Ingests new NPM packages from the Node Package Manager",
    group_name="npm",
    compute_kind="python",
)
def npm_package_ingestion(
    context: AssetExecutionContext,
) -> Output[dict[str, Any]]:
    try:
        logger.info("Starting NPM package ingestion process")

        npm_svc = get_npm_service()

        async def _run():
            await get_db().initialize()

            package_svc = get_package_service()
            version_svc = get_version_service()
            attr = get_attributor()

            new_packages = 0
            skipped_packages = 0
            error_count = 0

            all_package_names = await npm_svc.fetch_all_package_names()
            total_packages = len(all_package_names)

            logger.info(f"NPM - Found {total_packages} packages in the registry")
            context.log.info(f"NPM - Found {total_packages} packages in the registry")

            for idx, package_name in enumerate(all_package_names, 1):
                try:
                    package_name_lower = package_name.lower()

                    existing_package = await package_svc.read_package_by_name(
                        "NPMPackage", package_name_lower
                    )

                    if existing_package:
                        skipped_packages += 1
                        if idx % 1000 == 0:
                            context.log.info(
                                f"NPM - Progress: {idx}/{total_packages} (New: {new_packages}, Skipped: {skipped_packages})"
                            )
                        continue

                    package_schema = NPMPackageSchema(name=package_name_lower)

                    extractor = NPMPackageExtractor(
                        package=package_schema,
                        package_service=package_svc,
                        version_service=version_svc,
                        npm_service=npm_svc,
                        attributor=attr,
                    )

                    await extractor.run()
                    new_packages += 1

                    context.log.info(
                        f"NPM - Ingested new package: {package_name_lower} ({new_packages} new packages)"
                    )

                    if idx % 100 == 0:
                        context.log.info(
                            f"NPM - Progress: {idx}/{total_packages} (New: {new_packages}, Skipped: {skipped_packages})"
                        )

                except Exception as e:
                    error_count += 1
                    logger.error(f"NPM - Error ingesting {package_name}: {e}")
                    context.log.error(f"NPM - Error ingesting {package_name}: {e}")

            logger.info(
                f"NPM ingestion process completed. New packages: {new_packages}, Skipped: {skipped_packages}, Errors: {error_count}"
            )

            return {
                "total_in_registry": total_packages,
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
        logger.error(f"NPM - Fatal error in ingestion process: {e}")
        raise


@asset(
    description="Updates NPM package versions in SecureChain graph",
    group_name="npm",
    compute_kind="python",
)
def npm_packages_updates(
    context: AssetExecutionContext,
) -> Output[dict[str, Any]]:
    try:
        logger.info("Starting NPM package version update process")

        npm_svc = get_npm_service()

        async def _run():
            await get_db().initialize()

            package_svc = get_package_service()
            version_svc = get_version_service()
            attr = get_attributor()

            updater = NPMVersionUpdater(npm_svc, package_svc, version_svc, attr)

            package_count = 0
            version_count = 0
            error_count = 0

            async for batch in package_svc.read_packages_in_batches(
                "NPMPackage", batch_size=100
            ):
                for pkg in batch:
                    try:
                        await updater.update_package_versions(pkg)
                        package_count += 1

                        versions = (
                            await version_svc.count_number_of_versions_by_package(
                                "NPMPackage", pkg["name"]
                            )
                        )
                        version_count += versions

                        context.log.info(
                            f"NPM - Successfully updated {pkg['name']} (Total: {package_count})"
                        )
                    except Exception as e:
                        error_count += 1
                        logger.error(f"NPM - Error updating {pkg['name']}: {e}")

            logger.info(
                f"NPM update process completed. Total packages: {package_count}"
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
        logger.error(f"NPM - Fatal error in update process: {e}")
        raise
