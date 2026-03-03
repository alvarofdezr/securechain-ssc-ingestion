from dagster import DefaultScheduleStatus, ScheduleDefinition

from src.dagster_app.assets import (
    cargo_package_ingestion,
    cargo_packages_updates,
    go_package_ingestion,
    go_packages_updates,
    maven_package_ingestion,
    maven_packages_updates,
    npm_package_ingestion,
    npm_packages_updates,
    nuget_package_ingestion,
    nuget_packages_updates,
    pypi_package_ingestion,
    pypi_packages_updates,
    redis_queue_processor,
    rubygems_package_ingestion,
    rubygems_packages_updates,
)

# ============================================================================
# INGESTION SCHEDULES (Weekly - Sundays at 2-8 AM, STOPPED by default)
# ============================================================================

pypi_ingestion_schedule = ScheduleDefinition(
    name="pypi_weekly_ingestion",
    target=pypi_package_ingestion,
    cron_schedule="0 2 * * 0",
    default_status=DefaultScheduleStatus.STOPPED,
    description="Ingests new PyPI packages weekly on Sundays at 2:00 AM",
)

npm_ingestion_schedule = ScheduleDefinition(
    name="npm_weekly_ingestion",
    target=npm_package_ingestion,
    cron_schedule="0 3 * * 0",
    default_status=DefaultScheduleStatus.STOPPED,
    description="Ingests new NPM packages weekly on Sundays at 3:00 AM",
)

maven_ingestion_schedule = ScheduleDefinition(
    name="maven_weekly_ingestion",
    target=maven_package_ingestion,
    cron_schedule="0 4 * * 0",
    default_status=DefaultScheduleStatus.STOPPED,
    description="Ingests new Maven packages weekly on Sundays at 4:00 AM",
)

nuget_ingestion_schedule = ScheduleDefinition(
    name="nuget_weekly_ingestion",
    target=nuget_package_ingestion,
    cron_schedule="0 5 * * 0",
    default_status=DefaultScheduleStatus.STOPPED,
    description="Ingests new NuGet packages weekly on Sundays at 5:00 AM",
)

cargo_ingestion_schedule = ScheduleDefinition(
    name="cargo_weekly_ingestion",
    target=cargo_package_ingestion,
    cron_schedule="0 6 * * 0",
    default_status=DefaultScheduleStatus.STOPPED,
    description="Ingests new Cargo packages weekly on Sundays at 6:00 AM",
)

rubygems_ingestion_schedule = ScheduleDefinition(
    name="rubygems_weekly_ingestion",
    target=rubygems_package_ingestion,
    cron_schedule="0 7 * * 0",
    default_status=DefaultScheduleStatus.STOPPED,
    description="Ingests new RubyGems packages weekly on Sundays at 7:00 AM",
)

go_ingestion_schedule = ScheduleDefinition(
    name="go_weekly_ingestion",
    target=go_package_ingestion,
    cron_schedule="0 8 * * 0",
    default_status=DefaultScheduleStatus.STOPPED,
    description="Ingests new Go packages weekly on Sundays at 8:00 AM",
)

# ============================================================================
# UPDATE SCHEDULES (Daily at 10AM-9PM every 2 hours, RUNNING by default)
# ============================================================================

pypi_schedule = ScheduleDefinition(
    name="pypi_daily_update",
    target=pypi_packages_updates,
    cron_schedule="0 10 * * *",
    default_status=DefaultScheduleStatus.RUNNING,
    description="Updates PyPI packages daily at 10:00 AM",
)

npm_schedule = ScheduleDefinition(
    name="npm_daily_update",
    target=npm_packages_updates,
    cron_schedule="0 12 * * *",
    default_status=DefaultScheduleStatus.RUNNING,
    description="Updates NPM packages daily at 12:00 PM",
)

maven_schedule = ScheduleDefinition(
    name="maven_daily_update",
    target=maven_packages_updates,
    cron_schedule="0 14 * * *",
    default_status=DefaultScheduleStatus.RUNNING,
    description="Updates Maven packages daily at 2:00 PM",
)

cargo_schedule = ScheduleDefinition(
    name="cargo_daily_update",
    target=cargo_packages_updates,
    cron_schedule="0 16 * * *",
    default_status=DefaultScheduleStatus.RUNNING,
    description="Updates Cargo packages daily at 4:00 PM",
)

rubygems_schedule = ScheduleDefinition(
    name="rubygems_daily_update",
    target=rubygems_packages_updates,
    cron_schedule="0 18 * * *",
    default_status=DefaultScheduleStatus.RUNNING,
    description="Updates RubyGems packages daily at 6:00 PM",
)

nuget_schedule = ScheduleDefinition(
    name="nuget_daily_update",
    target=nuget_packages_updates,
    cron_schedule="0 20 * * *",
    default_status=DefaultScheduleStatus.RUNNING,
    description="Updates NuGet packages daily at 8:00 PM",
)

go_update_schedule = ScheduleDefinition(
    name="go_daily_update",
    target=go_packages_updates,
    cron_schedule="0 21 * * *",
    default_status=DefaultScheduleStatus.RUNNING,
    description="Updates Go packages daily at 9:00 PM",
)

# ============================================================================
# REDIS QUEUE PROCESSOR (Every 5 minutes, RUNNING by default)
# ============================================================================

redis_queue_schedule = ScheduleDefinition(
    name="redis_queue_processor",
    target=redis_queue_processor,
    cron_schedule="*/5 * * * *",
    default_status=DefaultScheduleStatus.RUNNING,
    description="Processes package extraction messages from Redis queue every 5 minutes",
)

all_schedules = [
    # Ingestion schedules
    pypi_ingestion_schedule,
    npm_ingestion_schedule,
    maven_ingestion_schedule,
    nuget_ingestion_schedule,
    cargo_ingestion_schedule,
    rubygems_ingestion_schedule,
    go_ingestion_schedule,
    # Update schedules
    pypi_schedule,
    npm_schedule,
    maven_schedule,
    cargo_schedule,
    rubygems_schedule,
    nuget_schedule,
    go_update_schedule,
    # Redis queue processor
    redis_queue_schedule,
]
