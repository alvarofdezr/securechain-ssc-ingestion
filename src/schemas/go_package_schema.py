from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class GoPackageSchema(BaseModel):
    """
    Pydantic schema representing a Go module package node.

    Models the data contract between the SSC-Ingestion pipeline and the
    graph database layer. Field definitions mirror the structure used by other
    ecosystem schemas (Cargo, NPM, etc.) to ensure consistency across the
    package service interface.

    Attributes:
        name:           Canonical Go module path as declared in go.mod
                        (e.g. 'github.com/gin-gonic/gin').
        vendor:         Top-level host segment of the module path, used as a
                        coarse vendor identifier (e.g. 'github.com').
                        Defaults to 'n/a' when the module path cannot be split.
        repository_url: URL to the module's source repository or its pkg.go.dev
                        entry when the repository cannot be inferred directly.
        moment:         Timestamp of the last ingestion or update operation.
        import_names:   List of import paths exposed by the module. For most
                        modules this is a single entry identical to 'name', but
                        multi-package modules may expose several import paths.
    """

    model_config = ConfigDict(validate_assignment=True, str_strip_whitespace=True)

    name: str = Field(
        ..., description="Canonical Go module path as declared in go.mod."
    )
    vendor: str = Field(
        default="n/a",
        description="Top-level host segment of the module path (e.g. 'github.com').",
    )
    repository_url: str = Field(
        default="n/a",
        description="Source repository URL or pkg.go.dev fallback.",
    )
    moment: datetime = Field(
        default_factory=datetime.now,
        description="Timestamp of the last ingestion or update operation.",
    )
    import_names: list[str] = Field(
        default_factory=list,
        description="Go import paths exposed by this module.",
    )

    def to_dict(self) -> dict:
        """
        Serialise the schema to a plain dictionary suitable for persistence
        via PackageService.

        The moment field is intentionally kept as a datetime object rather than
        being converted to an ISO string, as the database driver handles
        datetime serialisation internally.

        Returns:
            A dictionary with all schema fields as key-value pairs.
        """
        return {
            "name": self.name,
            "vendor": self.vendor,
            "repository_url": self.repository_url,
            "moment": self.moment,
            "import_names": self.import_names,
        }
