from src.dagster_app.assets.cargo_assets import (
    cargo_package_ingestion,
    cargo_packages_updates,
)
from src.dagster_app.assets.go_assets import (
    go_package_ingestion,
    go_packages_updates,
)  # added for Go
from src.dagster_app.assets.maven_assets import (
    maven_package_ingestion,
    maven_packages_updates,
)
from src.dagster_app.assets.npm_assets import (
    npm_package_ingestion,
    npm_packages_updates,
)
from src.dagster_app.assets.nuget_assets import (
    nuget_package_ingestion,
    nuget_packages_updates,
)
from src.dagster_app.assets.pypi_assets import (
    pypi_package_ingestion,
    pypi_packages_updates,
)
from src.dagster_app.assets.redis_queue_assets import redis_queue_processor
from src.dagster_app.assets.rubygems_assets import (
    rubygems_package_ingestion,
    rubygems_packages_updates,
)

__all__ = [
    "cargo_package_ingestion",
    "cargo_packages_updates",
    "go_package_ingestion",  # added for Go
    "go_packages_updates",  # added for Go
    "maven_package_ingestion",
    "maven_packages_updates",
    "npm_package_ingestion",
    "npm_packages_updates",
    "nuget_package_ingestion",
    "nuget_packages_updates",
    "pypi_package_ingestion",
    "pypi_packages_updates",
    "redis_queue_processor",
    "rubygems_package_ingestion",
    "rubygems_packages_updates",
]
