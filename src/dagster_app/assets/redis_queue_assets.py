from asyncio import run
from json import JSONDecodeError, loads
from typing import Any

from dagster import AssetExecutionContext, MetadataValue, Output, asset
from pydantic import ValidationError

from src.dependencies import (
    get_attributor,
    get_cargo_service,
    get_db,
    get_maven_service,
    get_npm_service,
    get_nuget_service,
    get_package_service,
    get_pypi_service,
    get_rubygems_service,
    get_go_service, #added for Go, using a common service getter pattern
    get_version_service,
)
from src.logger import logger
from src.processes.extractors import (
    CargoPackageExtractor,
    MavenPackageExtractor,
    NPMPackageExtractor,
    NuGetPackageExtractor,
    PyPIPackageExtractor,
    RubyGemsPackageExtractor,
    GoPackageExtractor, 
)
from src.schemas import (
    CargoPackageSchema,
    MavenPackageSchema,
    NPMPackageSchema,
    NuGetPackageSchema,
    PackageMessageSchema,
    PyPIPackageSchema,
    RubyGemsPackageSchema,
    GoPackageSchema, 
)
from src.utils import RedisQueue


@asset(
    description="Processes package extraction messages from Redis queue",
    group_name="redis_queue",
    compute_kind="python",
)
def redis_queue_processor(
    context: AssetExecutionContext,
) -> Output[dict[str, Any]]:
    try:
        logger.info("Starting Redis queue processor")
        context.log.info("Connecting to Redis queue...")

        redis_queue = RedisQueue.from_env()

        pypi_svc = get_pypi_service()
        npm_svc = get_npm_service()
        maven_svc = get_maven_service()
        nuget_svc = get_nuget_service()
        cargo_svc = get_cargo_service()
        rubygems_svc = get_rubygems_service()
        go_svc = get_go_service() #

        total_processed = 0
        successful = 0
        failed = 0
        validation_errors = 0
        unsupported_types = 0

        async def _process_messages():
            nonlocal total_processed, successful, failed, validation_errors, unsupported_types

            db = get_db()
            await db.initialize()

            package_svc = get_package_service()
            version_svc = get_version_service()
            attr = get_attributor()

            extractor_map = {
                "PyPIPackage": {
                    "extractor_class": PyPIPackageExtractor,
                    "schema_class": PyPIPackageSchema,
                    "service": pypi_svc,
                    "service_param": "pypi_service",
                },
                "NPMPackage": {
                    "extractor_class": NPMPackageExtractor,
                    "schema_class": NPMPackageSchema,
                    "service": npm_svc,
                    "service_param": "npm_service",
                },
                "MavenPackage": {
                    "extractor_class": MavenPackageExtractor,
                    "schema_class": MavenPackageSchema,
                    "service": maven_svc,
                    "service_param": "maven_service",
                },
                "NuGetPackage": {
                    "extractor_class": NuGetPackageExtractor,
                    "schema_class": NuGetPackageSchema,
                    "service": nuget_svc,
                    "service_param": "nuget_service",
                },
                "CargoPackage": {
                    "extractor_class": CargoPackageExtractor,
                    "schema_class": CargoPackageSchema,
                    "service": cargo_svc,
                    "service_param": "cargo_service",
                },
                "RubyGemsPackage": {
                    "extractor_class": RubyGemsPackageExtractor,
                    "schema_class": RubyGemsPackageSchema,
                    "service": rubygems_svc,
                    "service_param": "rubygems_service",
                },
                "GoPackage": {
                    "extractor_class": GoPackageExtractor,
                    "schema_class": GoPackageSchema,
                    "service": go_svc,
                    "service_param": "go_service",
                },
            }

            messages = redis_queue.read_batch(count=100, block_ms=1000)

            if not messages:
                logger.info("No messages in Redis queue")
                context.log.info("No messages found in Redis queue")
                return

            logger.info(f"Processing {len(messages)} messages from Redis queue")
            context.log.info(f"Processing {len(messages)} messages from Redis queue")

            for msg_id, raw_json in messages:
                total_processed += 1

                try:
                    data = loads(raw_json)

                    try:
                        message = PackageMessageSchema(**data)
                    except ValidationError as e:
                        validation_errors += 1
                        error_msg = f"Validation error: {e}"
                        logger.error(f"Message {msg_id}: {error_msg}")
                        context.log.error(f"Message {msg_id}: {error_msg}")
                        redis_queue.dead_letter(msg_id, raw_json, error_msg)
                        continue

                    if message.node_type not in extractor_map:
                        unsupported_types += 1
                        error_msg = f"Unsupported node_type: {message.node_type}"
                        logger.warning(f"Message {msg_id}: {error_msg}")
                        context.log.warning(f"Message {msg_id}: {error_msg}")
                        redis_queue.dead_letter(msg_id, raw_json, error_msg)
                        continue

                    extractor_config = extractor_map[message.node_type]

                    if message.node_type == "MavenPackage":
                        parts = message.package.split(":")
                        if len(parts) != 2:
                            error_msg = f"Invalid Maven package format: {message.package} (expected 'group_id:artifact_id')"
                            logger.error(f"Message {msg_id}: {error_msg}")
                            context.log.error(f"Message {msg_id}: {error_msg}")
                            redis_queue.dead_letter(msg_id, raw_json, error_msg)
                            validation_errors += 1
                            continue

                        group_id, artifact_id = parts
                        package_schema = extractor_config["schema_class"](
                            group_id=group_id,
                            artifact_id=artifact_id,
                            name=message.package,
                            vendor=message.vendor,
                            repository_url=str(message.repository_url) if message.repository_url else "",
                        )
                    else:
                        package_schema = extractor_config["schema_class"](
                            name=message.package,
                            vendor=message.vendor,
                            repository_url=str(message.repository_url) if message.repository_url else "",
                        )

                    extractor_params = {
                        "package": package_schema,
                        "package_service": package_svc,
                        "version_service": version_svc,
                        extractor_config["service_param"]: extractor_config["service"],
                        "attributor": attr,
                    }

                    if message.constraints:
                        extractor_params["constraints"] = message.constraints
                    if message.parent_id:
                        extractor_params["parent_id"] = message.parent_id
                    if message.parent_version:
                        extractor_params["parent_version"] = message.parent_version
                    if message.refresh:
                        extractor_params["refresh"] = message.refresh

                    extractor = extractor_config["extractor_class"](**extractor_params)

                    await extractor.run()

                    redis_queue.ack(msg_id)
                    successful += 1

                    logger.info(
                        f"Successfully processed {message.node_type} package: {message.package}"
                    )
                    context.log.info(
                        f"Processed {message.node_type}: {message.package} ({successful}/{total_processed})"
                    )

                except JSONDecodeError as e:
                    failed += 1
                    error_msg = f"JSON decode error: {e}"
                    logger.error(f"Message {msg_id}: {error_msg}")
                    context.log.error(f"Message {msg_id}: {error_msg}")
                    redis_queue.dead_letter(msg_id, raw_json, error_msg)

                except Exception as e:
                    failed += 1
                    error_msg = f"Processing error: {e}"
                    logger.error(f"Message {msg_id}: {error_msg}", exc_info=True)
                    context.log.error(f"Message {msg_id}: {error_msg}")
                    redis_queue.dead_letter(msg_id, raw_json, error_msg)

            logger.info(
                f"Redis queue processing completed. "
                f"Total: {total_processed}, Success: {successful}, Failed: {failed}, "
                f"Validation errors: {validation_errors}, Unsupported: {unsupported_types}"
            )

        run(_process_messages())

        success_rate = (successful / total_processed * 100) if total_processed > 0 else 0.0

        return Output(
            value={
                "total_processed": total_processed,
                "successful": successful,
                "failed": failed,
                "validation_errors": validation_errors,
                "unsupported_types": unsupported_types,
                "success_rate": success_rate,
            },
            metadata={
                "total_processed": total_processed,
                "successful": successful,
                "failed": failed,
                "validation_errors": validation_errors,
                "unsupported_types": unsupported_types,
                "success_rate": MetadataValue.float(success_rate),
                "message": f"Processed {total_processed} messages from Redis queue",
            },
        )

    except Exception as e:
        logger.error(f"Fatal error in Redis queue processor: {e}", exc_info=True)
        context.log.error(f"Fatal error in Redis queue processor: {e}")
        raise
