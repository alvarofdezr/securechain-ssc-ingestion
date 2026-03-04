from asyncio import run
from typing import Any

from dagster import AssetExecutionContext, MetadataValue, Output, asset

from src.dependencies import (
    get_attributor,
    get_db,
    get_maven_service,
    get_package_service,
    get_version_service,
)
from src.logger import logger
from src.processes.extractors import MavenPackageExtractor
from src.processes.updaters import MavenVersionUpdater
from src.schemas import MavenPackageSchema


@asset(
    description="Ingests new Maven packages from Maven Central",
    group_name="maven",
    compute_kind="python",
)
def maven_package_ingestion(
    context: AssetExecutionContext,
) -> Output[dict[str, Any]]:
    try:
        logger.info("Starting Maven package ingestion process")

        maven_svc = get_maven_service()

        async def _run():
            await get_db().initialize()

            package_svc = get_package_service()
            version_svc = get_version_service()
            attr = get_attributor()

            new_packages = 0
            skipped_packages = 0
            error_count = 0

            logger.info("Maven - Fetching all package names from Maven Central...")
            context.log.info("Maven - Fetching all package names from Maven Central...")

            all_packages = await maven_svc.fetch_all_package_names()
            total_packages = len(all_packages)

            logger.info(f"Maven - Found {total_packages} packages in Maven Central")
            context.log.info(
                f"Maven - Found {total_packages} packages in Maven Central"
            )

            for idx, package_name in enumerate(all_packages, 1):
                try:
                    if ":" not in package_name:
                        continue

                    group_id, artifact_id = package_name.split(":", 1)

                    if not group_id or not artifact_id:
                        continue

                    existing_package = await package_svc.read_package_by_name(
                        "MavenPackage", package_name
                    )

                    if existing_package:
                        skipped_packages += 1
                        if idx % 1000 == 0:
                            context.log.info(
                                f"Maven - Progress: {idx}/{total_packages} (New: {new_packages}, Skipped: {skipped_packages})"
                            )
                        continue

                    package_schema = MavenPackageSchema(
                        group_id=group_id,
                        artifact_id=artifact_id,
                        name=package_name,
                    )

                    extractor = MavenPackageExtractor(
                        package=package_schema,
                        package_service=package_svc,
                        version_service=version_svc,
                        maven_service=maven_svc,
                        attributor=attr,
                    )

                    await extractor.run()
                    new_packages += 1

                    context.log.info(
                        f"Maven - Ingested new package: {package_name} ({new_packages} new packages)"
                    )

                    if idx % 100 == 0:
                        context.log.info(
                            f"Maven - Progress: {idx}/{total_packages} (New: {new_packages}, Skipped: {skipped_packages})"
                        )

                except Exception as e:
                    error_count += 1
                    logger.error(f"Maven - Error ingesting {package_name}: {e}")
                    context.log.error(f"Maven - Error ingesting {package_name}: {e}")

            logger.info(
                f"Maven ingestion process completed. New packages: {new_packages}, Skipped: {skipped_packages}, Errors: {error_count}"
            )

            return {
                "total_in_central": total_packages,
                "new_packages_ingested": new_packages,
                "skipped_existing": skipped_packages,
                "errors": error_count,
            }

        stats = run(_run())

        return Output(
            value=stats,
            metadata={
                "total_in_central": stats["total_in_central"],
                "new_packages_ingested": stats["new_packages_ingested"],
                "skipped_existing": stats["skipped_existing"],
                "errors": stats["errors"],
                "ingestion_rate": MetadataValue.float(
                    (stats["new_packages_ingested"] / stats["total_in_central"] * 100)
                    if stats["total_in_central"] > 0
                    else 0.0
                ),
            },
        )

    except Exception as e:
        logger.error(f"Maven - Fatal error in ingestion process: {e}")
        raise


@asset(
    description="Updates Maven package versions in SecureChain graph",
    group_name="maven",
    compute_kind="python",
)
def maven_packages_updates(
    context: AssetExecutionContext,
) -> Output[dict[str, Any]]:
    try:
        logger.info("Starting Maven package version update process")

        maven_svc = get_maven_service()

        async def _run():
            await get_db().initialize()

            package_svc = get_package_service()
            version_svc = get_version_service()
            attr = get_attributor()

            updater = MavenVersionUpdater(maven_svc, package_svc, version_svc, attr)

            package_count = 0
            version_count = 0
            error_count = 0

            async for batch in package_svc.read_packages_in_batches(
                "MavenPackage", batch_size=100
            ):
                for pkg in batch:
                    try:
                        await updater.update_package_versions(pkg)
                        package_count += 1

                        versions = (
                            await version_svc.count_number_of_versions_by_package(
                                "MavenPackage", pkg["name"]
                            )
                        )
                        version_count += versions

                        context.log.info(
                            f"Maven - Successfully updated {pkg['name']} (Total: {package_count})"
                        )
                    except Exception as e:
                        error_count += 1
                        logger.error(f"Maven - Error updating {pkg['name']}: {e}")

            logger.info(
                f"Maven update process completed. Total packages: {package_count}"
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
        logger.error(f"Maven - Fatal error in update process: {e}")
        raise
