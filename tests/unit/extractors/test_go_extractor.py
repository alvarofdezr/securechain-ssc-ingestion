from unittest.mock import AsyncMock, MagicMock

import pytest

from src.processes.extractors.go_extractor import GoPackageExtractor
from src.schemas.go_package_schema import GoPackageSchema


@pytest.fixture
def go_service_mock():
    """Mocks the GoService for extractor tests."""
    mock = MagicMock()
    mock.get_versions = AsyncMock(
        return_value=[
            {"name": "v1.0.0", "serial_number": 1},
            {"name": "v1.1.0", "serial_number": 2},
        ]
    )
    mock.get_import_names = AsyncMock(return_value=["test/pkg"])
    mock.get_repo_url = MagicMock(return_value="https://github.com/test/pkg")
    mock.get_package_requirements = AsyncMock(
        return_value={
            "known/pkg": "v1.0.0",
            "unknown/pkg": "v2.0.0",
        }
    )
    return mock


@pytest.fixture
def package_service_mock():
    """Mocks the PackageService for extractor tests."""
    mock = MagicMock()
    mock.create_package_and_versions = AsyncMock(return_value=[])
    mock.read_package_by_name = AsyncMock(
        side_effect=lambda type, name: {"name": name, "id": "some-id"} if name == "known/pkg" else None
    )
    mock.relate_packages = AsyncMock()
    mock.update_package_moment = AsyncMock()
    return mock


@pytest.fixture
def version_service_mock():
    """Mocks the VersionService for extractor tests."""
    mock = MagicMock()
    mock.update_versions_serial_number = AsyncMock()
    return mock


@pytest.fixture
def attributor_mock():
    """Mocks the Attributor for extractor tests."""
    mock = MagicMock()
    mock.attribute_vulnerabilities = AsyncMock(side_effect=lambda name, v: v)
    return mock


@pytest.fixture
def go_extractor(
    go_service_mock, package_service_mock, version_service_mock, attributor_mock
):
    """Provides a GoPackageExtractor instance with mocked dependencies."""
    schema = GoPackageSchema(name="test/pkg")
    return GoPackageExtractor(
        package=schema,
        package_service=package_service_mock,
        version_service=version_service_mock,
        go_service=go_service_mock,
        attributor=attributor_mock,
    )


@pytest.mark.asyncio
async def test_run_calls_create_package(go_extractor, package_service_mock):
    """
    Verifies that the extractor's run method correctly orchestrates the
    package creation process by calling create_package_and_versions with the
    right node type.
    """
    await go_extractor.run()
    package_service_mock.create_package_and_versions.assert_called_once()
    call_args = package_service_mock.create_package_and_versions.call_args[0]
    assert call_args[0] == "GoPackage"


@pytest.mark.asyncio
async def test_create_package_skips_on_empty_versions(
    go_extractor, go_service_mock, package_service_mock
):
    """
    Ensures that if a package has no versions, the extractor skips creation
    and does not raise an exception, preventing empty nodes in the graph.
    """
    go_service_mock.get_versions.return_value = []
    await go_extractor.run()
    package_service_mock.create_package_and_versions.assert_not_called()


@pytest.mark.asyncio
async def test_generate_packages_relates_known_packages(
    go_extractor, package_service_mock
):
    """
    Tests the dependency resolution logic. Verifies that if a dependency
    already exists in the graph, it is related to the parent package.
    If it does not exist, it is created (via create_package_and_versions).
    """
    await go_extractor.generate_packages(
        {"known/pkg": "v1.0.0", "unknown/pkg": "v2.0.0"},
        parent_id="some-id",
        parent_version_name="v1.1.0",
    )

    assert package_service_mock.read_package_by_name.call_count == 2

    package_service_mock.relate_packages.assert_called_once()
    related_pkgs = package_service_mock.relate_packages.call_args[0][1]
    assert any(p["name"] == "known/pkg" for p in related_pkgs)

    package_service_mock.create_package_and_versions.assert_called_once()
    call_args = package_service_mock.create_package_and_versions.call_args[0]
    assert call_args[0] == "GoPackage"

@pytest.mark.asyncio
async def test_cycle_protection_stops_infinite_recursion(
    go_extractor, go_service_mock, package_service_mock
):
    """
    Validates that when the maximum recursion depth is reached, the extractor 
    does not attempt to create new packages for unknown dependencies, which 
    prevents infinite loops in cases of cyclic dependencies
    """
    go_service_mock.get_package_requirements = AsyncMock(
        return_value={"test/pkg": "v1.0.0"} 
    )
    package_service_mock.create_package_and_versions = AsyncMock(
        return_value=[{"name": "v1.0.0", "id": "id-1"}]
    )
    package_service_mock.read_package_by_name = AsyncMock(return_value=None)

    await go_extractor.run()
    assert package_service_mock.create_package_and_versions.call_count == 1


@pytest.mark.asyncio
async def test_only_latest_version_dependencies_resolved(
    go_extractor, go_service_mock, package_service_mock
):
    """
    Verifies that only the latest version of a package is processed for
    dependencies. Older versions should not trigger dependency resolution, which    
    prevents redundant work and potential cycles in the graph.
    """
    go_service_mock.get_versions = AsyncMock(return_value=[
        {"name": "v1.0.0", "serial_number": 0},
        {"name": "v1.1.0", "serial_number": 1},
        {"name": "v1.2.0", "serial_number": 2},
    ])
    package_service_mock.create_package_and_versions = AsyncMock(
        return_value=[
            {"name": "v1.0.0", "id": "id-1"},
            {"name": "v1.1.0", "id": "id-2"},
            {"name": "v1.2.0", "id": "id-3"},
        ]
    )

    go_service_mock.get_package_requirements = AsyncMock(return_value={})

    await go_extractor.run()

    assert go_service_mock.get_package_requirements.call_count == 1
    call_args = go_service_mock.get_package_requirements.call_args
    assert call_args[0][1] == "v1.2.0"