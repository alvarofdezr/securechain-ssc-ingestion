
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.processes.updaters.go_version_updater import GoVersionUpdater


@pytest.fixture
def go_service_mock():
    """Mocks the GoService, returning a predefined version list."""
    mock = MagicMock()
    mock.get_versions = AsyncMock(
        return_value=[
            {"name": "v1.0.0", "serial_number": 1},
            {"name": "v1.1.0", "serial_number": 2},
            {"name": "v1.2.0", "serial_number": 3},
        ]
    )
    return mock


@pytest.fixture
def package_service_mock():
    """Mocks the PackageService."""
    return MagicMock()


@pytest.fixture
def version_service_mock():
    """Mocks the VersionService."""
    mock = MagicMock()
    mock.count_number_of_versions_by_package = AsyncMock(return_value=3)
    mock.read_versions_names_by_package = AsyncMock(return_value=["v1.0.0"])
    mock.create_versions = AsyncMock()
    return mock


@pytest.fixture
def attributor_mock():
    """Mocks the Attributor."""
    return MagicMock()


@pytest.mark.asyncio
async def test_update_skips_when_count_matches(
    go_service_mock, package_service_mock, version_service_mock, attributor_mock
):
    """
    Verifies that the updater correctly skips processing when the number of
    versions in the database matches the number reported by the proxy,
    avoiding redundant work.
    """
    updater = GoVersionUpdater(
        go_service_mock, package_service_mock, version_service_mock, attributor_mock
    )
    await updater.update_package_versions({"name": "test/pkg"})
    version_service_mock.create_versions.assert_not_called()


@pytest.mark.asyncio
async def test_update_creates_only_new_versions(
    go_service_mock, package_service_mock, version_service_mock, attributor_mock
):
    """
    Ensures that when new versions are detected, only the missing ones are
    passed to create_versions, preventing duplicate version entries.
    """
    version_service_mock.count_number_of_versions_by_package.return_value = 1
    updater = GoVersionUpdater(
        go_service_mock, package_service_mock, version_service_mock, attributor_mock
    )

    await updater.update_package_versions({"name": "test/pkg"})

    version_service_mock.create_versions.assert_called_once()
    call_args = version_service_mock.create_versions.call_args[0]
    new_versions = call_args[1]
    assert len(new_versions) == 2
    assert {v["name"] for v in new_versions} == {"v1.1.0", "v1.2.0"}


@pytest.mark.asyncio
async def test_update_does_not_mutate_versions_list_during_iteration(
    go_service_mock, package_service_mock, version_service_mock, attributor_mock
):
    """
    This test verifies a fix for a potential bug where modifying a list while
    iterating over it can cause items to be skipped. It confirms that all
    new versions are correctly identified and processed even when the list
    of existing versions is a subset of the full list.
    """
    go_service_mock.get_versions.return_value = [
        {"name": "v1.0.0", "serial_number": 1},
        {"name": "v1.1.0", "serial_number": 2},
        {"name": "v1.2.0", "serial_number": 3},
        {"name": "v1.3.0", "serial_number": 4},
    ]
    version_service_mock.count_number_of_versions_by_package.return_value = 2
    version_service_mock.read_versions_names_by_package.return_value = [
        "v1.0.0",
        "v1.2.0",
    ]
    updater = GoVersionUpdater(
        go_service_mock, package_service_mock, version_service_mock, attributor_mock
    )

    await updater.update_package_versions({"name": "test/pkg"})

    version_service_mock.create_versions.assert_called_once()
    new_versions = version_service_mock.create_versions.call_args[0][1]
    assert len(new_versions) == 2
    assert {v["name"] for v in new_versions} == {"v1.1.0", "v1.3.0"}
