
import pytest
from src.schemas.go_package_schema import GoPackageSchema


def test_schema_defaults():
    """
    Verifies that the GoPackageSchema correctly applies default values for
    optional fields when they are not provided on instantiation.
    """
    schema = GoPackageSchema(name="my/package")
    assert schema.vendor == "n/a"
    assert schema.repository_url == "n/a"
    assert schema.import_names == []


def test_to_dict_contains_all_fields():
    """
    Ensures that the to_dict method includes all expected fields from the
    schema, which is critical for consistent data representation in the graph.
    """
    schema = GoPackageSchema(name="my/package")
    data = schema.to_dict()
    expected_keys = ["name", "vendor", "repository_url", "moment", "import_names"]
    assert all(key in data for key in expected_keys)
    assert len(data.keys()) == len(expected_keys)


def test_schema_strips_whitespace():
    """
    Tests that leading/trailing whitespace is stripped from the 'name' field
    to ensure canonical and clean module path storage.
    """
    schema = GoPackageSchema(name="  github.com/foo/bar  ")
    assert schema.name == "github.com/foo/bar"

