from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class PackageMessageSchema(BaseModel):
    """Schema for asynchronous package analysis messages in the processing queue.

    Defines the structure of package metadata and context for asynchronous
    ingestion and analysis operations. Messages are enqueued in Redis Streams
    for consumption by workers that fetch package data, attribute vulnerabilities,
    and persist package information to the knowledge graph.

    Enables both primary package ingestion (parent_id and parent_version are None)
    and transitive dependency analysis (when linked to a parent package version).

    Configuration:
    - Assignment validation enabled: field changes are validated after initialization.
    - Automatic whitespace stripping: string fields are trimmed of leading/trailing spaces.

    Attributes:
        node_type: Package manager ecosystem classification
            (e.g., 'PyPIPackage', 'NPMPackage', 'MavenPackage', 'GoPackage').
            Used to determine version parsing semantics and constraint dialects.
        package: Package name or identifier as registered in the package manager
            registry (e.g., 'requests', '@angular/core', 'org.springframework:spring-core').
        vendor: Package vendor or organization name. Defaults to 'n/a' if unknown
            or not applicable. Used for vendor-based filtering and attribution.
        repository_url: URL to the package source repository (e.g., GitHub, GitLab).
            Defaults to empty string. Used for enriching package metadata.
        moment: Timestamp of message creation. Defaults to current UTC time.
            Used for tracking ingestion order and timing analysis.
        constraints: Optional version constraint specification for the package
            (e.g., '>=1.0.0', '^2.1', 'latest'). If None, the latest version is assumed.
        parent_id: Element ID of the parent package version, if this message
            represents a transitive dependency. None for root package ingestions.
        parent_version: Semantic version identifier of the parent package, if present.
            Used for tracking dependency relationships in the knowledge graph.
            None for root packages.
        refresh: Boolean flag indicating whether to refresh an existing package
            regardless of its current state. Forces re-attribution and re-discovery
            even if the package is already persisted (default: False).
    """
    model_config = ConfigDict(validate_assignment=True, str_strip_whitespace=True)

    node_type: str = Field(
        ..., description="Package manager (e.g., PyPIPackage, NPMPackage, ...)"
    )
    package: str = Field(..., description="Package name")
    vendor: str = Field("n/a", description="Package vendor")
    repository_url: str | None = Field(default="", description="Repository URL")
    moment: datetime = Field(default_factory=datetime.now)
    constraints: str | None = None
    parent_id: str | None = None
    parent_version: str | None = None
    refresh: bool = False
