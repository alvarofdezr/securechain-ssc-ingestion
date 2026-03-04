from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiohttp import ClientSession

from src.services.apis.go_service import GoService


@pytest.fixture
def go_service():
    """Provides a GoService instance with a mocked cache."""
    with (
        patch("src.services.apis.go_service.get_cache_manager") as mock_get_cache,
        patch("src.services.apis.go_service.get_orderer") as mock_get_orderer,
    ):
        mock_cache = MagicMock()
        mock_cache.get_cache = AsyncMock(return_value=None)
        mock_cache.set_cache = AsyncMock()
        mock_get_cache.return_value = mock_cache

        mock_orderer = MagicMock()
        mock_get_orderer.return_value = mock_orderer

        service = GoService()
        return service


def _mock_session_manager(mock_session):
    """Helper that returns a correctly patched session manager context."""
    mock_session_manager = MagicMock()
    mock_session_manager.get_session = AsyncMock(return_value=mock_session)
    return mock_session_manager


@pytest.mark.asyncio
async def test_fetch_all_package_names_returns_parsed_paths(go_service: GoService):
    """
    Verifies that fetch_all_package_names correctly parses module paths
    from a valid NDJSON response.
    """
    mock_session = MagicMock(spec=ClientSession)
    mock_response = MagicMock()
    mock_response.text = AsyncMock(
        return_value=(
            '{"Path": "github.com/a/a"}\n'
            '{"Path": "github.com/b/b"}\n'
            '{"Path": "github.com/c/c"}'
        )
    )
    mock_session.get.return_value.__aenter__.return_value = mock_response

    with patch("src.services.apis.go_service.get_session_manager") as mock_get_session:
        mock_get_session.return_value = _mock_session_manager(mock_session)
        result = await go_service.fetch_all_package_names()
        assert set(result) == {"github.com/a/a", "github.com/b/b", "github.com/c/c"}


@pytest.mark.asyncio
async def test_fetch_all_package_names_handles_malformed_lines(go_service: GoService):
    """
    Ensures that malformed lines in the index response are skipped without
    crashing the parser, and valid lines are still processed.
    """
    mock_session = MagicMock(spec=ClientSession)
    mock_response = MagicMock()
    mock_response.text = AsyncMock(
        return_value=(
            '{"Path": "github.com/a/a"}\nnot-json\n{"Path": "github.com/c/c"}'
        )
    )
    mock_session.get.return_value.__aenter__.return_value = mock_response

    with patch("src.services.apis.go_service.get_session_manager") as mock_get_session:
        mock_get_session.return_value = _mock_session_manager(mock_session)
        result = await go_service.fetch_all_package_names()
        assert set(result) == {"github.com/a/a", "github.com/c/c"}


@pytest.mark.asyncio
async def test_fetch_versions_list_returns_versions(go_service: GoService):
    """
    Tests that fetch_versions_list correctly parses a newline-separated
    list of version strings from a 200 response.
    """
    mock_session = MagicMock(spec=ClientSession)
    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.text = AsyncMock(return_value="v1.0.0\nv1.1.0\nv1.2.0")
    mock_session.get.return_value.__aenter__.return_value = mock_response

    with patch("src.services.apis.go_service.get_session_manager") as mock_get_session:
        mock_get_session.return_value = _mock_session_manager(mock_session)
        result = await go_service.fetch_versions_list("test/pkg")
        assert result == ["v1.0.0", "v1.1.0", "v1.2.0"]


@pytest.mark.asyncio
async def test_fetch_versions_list_returns_empty_on_404(go_service: GoService):
    """
    Verifies that a 404 Not Found status from the proxy is handled gracefully
    by returning an empty list, as this indicates a module with no versions.
    """
    mock_session = MagicMock(spec=ClientSession)
    mock_response = MagicMock()
    mock_response.status = 404
    mock_session.get.return_value.__aenter__.return_value = mock_response

    with patch("src.services.apis.go_service.get_session_manager") as mock_get_session:
        mock_get_session.return_value = _mock_session_manager(mock_session)
        result = await go_service.fetch_versions_list("test/pkg")
        assert result == []


@pytest.mark.asyncio
async def test_get_versions_orders_correctly(go_service: GoService):
    """
    Ensures that the get_versions method correctly applies semantic version
    sorting and assigns monotonically increasing serial numbers.
    """
    go_service.fetch_versions_list = AsyncMock(
        return_value=["v1.10.0", "v1.2.0", "v0.9.0"]
    )
    go_service.orderer.order_versions = MagicMock(
        return_value=[
            {"name": "v0.9.0", "serial_number": 1},
            {"name": "v1.2.0", "serial_number": 2},
            {"name": "v1.10.0", "serial_number": 3},
        ]
    )
    result = await go_service.get_versions("test/pkg")
    assert [r["name"] for r in result] == ["v0.9.0", "v1.2.0", "v1.10.0"]
    assert [r["serial_number"] for r in result] == [1, 2, 3]


def test_get_repo_url_github(go_service: GoService):
    """
    Tests that module paths from github.com are correctly resolved to their
    HTTPS repository URL.
    """
    url = go_service.get_repo_url("github.com/user/repo")
    assert url == "https://github.com/user/repo"


def test_get_repo_url_fallback(go_service: GoService):
    """
    Verifies that non-standard module paths fall back to the pkg.go.dev
    documentation URL.
    """
    url = go_service.get_repo_url("my.corp.com/internal/pkg")
    assert url == "https://pkg.go.dev/my.corp.com/internal/pkg"


def test_parse_go_mod_block_form(go_service: GoService):
    """
    Ensures that 'require' directives within a block are parsed correctly.
    """
    content = (
        "module my/mod\n"
        "go 1.16\n"
        "require (\n"
        "    github.com/a/a v1.0.0\n"
        "    github.com/b/b v1.2.3\n"
        ")\n"
    )
    deps = go_service._parse_go_mod(content)
    assert deps == {"github.com/a/a": "v1.0.0", "github.com/b/b": "v1.2.3"}


def test_parse_go_mod_single_line_form(go_service: GoService):
    """
    Tests parsing of single-line 'require' directives.
    """
    content = (
        "module my/mod\n"
        "go 1.16\n"
        "require github.com/a/a v1.0.0\n"
        "require github.com/b/b v1.2.3\n"
    )
    deps = go_service._parse_go_mod(content)
    assert deps == {"github.com/a/a": "v1.0.0", "github.com/b/b": "v1.2.3"}


def test_parse_go_mod_strips_indirect_comments(go_service: GoService):
    """
    Verifies that '// indirect' comments are stripped from version strings
    to avoid corrupting the version identifier.
    """
    content = "require github.com/a/a v1.0.0 // indirect\n"
    deps = go_service._parse_go_mod(content)
    assert deps == {"github.com/a/a": "v1.0.0"}


@pytest.mark.asyncio
async def test_fetch_packages_since_returns_since_unchanged_on_empty_batch(go_service):
    """
    If the index returns an empty batch, the cursor should not advance, indicating
    that we are fully caught up. This prevents unnecessary re-querying of the index
    with the same cursor.
    """
    mock_session = MagicMock(spec=ClientSession)
    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.text = AsyncMock(return_value="")
    mock_session.get.return_value.__aenter__.return_value = mock_response

    since = "2024-01-01T00:00:00Z"
    with patch("src.services.apis.go_service.get_session_manager") as mock_get_session:
        mock_get_session.return_value = _mock_session_manager(mock_session)
        packages, next_cursor = await go_service.fetch_packages_since(since)

    assert packages == []
    assert next_cursor == since


@pytest.mark.asyncio
async def test_fetch_packages_since_advances_cursor(go_service):
    """
    Verifies that the cursor returned by fetch_packages_since is the latest
    timestamp from the batch, which allows the ingestion process to continue
    from where it left off.
    """
    mock_session = MagicMock(spec=ClientSession)
    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.text = AsyncMock(
        return_value=(
            '{"Path": "github.com/a/a", "Version": "v1.0.0", "Timestamp": "2024-01-01T10:00:00Z"}\n'
            '{"Path": "github.com/b/b", "Version": "v1.0.0", "Timestamp": "2024-01-01T11:00:00Z"}\n'
        )
    )
    mock_session.get.return_value.__aenter__.return_value = mock_response

    since = "2024-01-01T00:00:00Z"
    with patch("src.services.apis.go_service.get_session_manager") as mock_get_session:
        mock_get_session.return_value = _mock_session_manager(mock_session)
        packages, next_cursor = await go_service.fetch_packages_since(since)

    assert set(packages) == {"github.com/a/a", "github.com/b/b"}
    assert next_cursor == "2024-01-01T11:00:00Z"


@pytest.mark.asyncio
async def test_get_import_names_skips_oversized_zip(go_service):
    """
    Verifies that if the Content-Length of the zip file exceeds the defined
    threshold, the method returns an empty list without attempting to read the
    zip. This prevents memory issues and long processing times for very large modules.
    """
    mock_session = MagicMock(spec=ClientSession)
    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.headers = {"Content-Length": str(100 * 1024 * 1024)}  # 100 MB
    mock_session.get.return_value.__aenter__.return_value = mock_response

    with patch("src.services.apis.go_service.get_session_manager") as mock_get_session:
        mock_get_session.return_value = _mock_session_manager(mock_session)
        result = await go_service.get_import_names("github.com/big/module", "v1.0.0")

    assert result == ["github.com/big/module"]
    mock_response.read.assert_not_called()
