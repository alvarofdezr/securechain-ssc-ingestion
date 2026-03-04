from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class MavenPackageSchema(BaseModel):
    model_config = ConfigDict(validate_assignment=True, str_strip_whitespace=True)

    group_id: str = Field(..., description="Group ID of the Maven package")
    artifact_id: str = Field(..., description="Artifact ID of the Maven package")
    name: str = Field(
        ..., description="Full name of the Maven package (group_id:artifact_id)"
    )
    vendor: str = Field(default="n/a", description="Vendor source")
    repository_url: str = Field(
        default="n/a", description="Repository URL (GitHub/GitLab/Bitbucket)"
    )
    moment: datetime = Field(
        default_factory=datetime.now, description="Timestamp of the last update"
    )
    import_names: list[str] = Field(
        default_factory=list,
        description="List of Java package names extracted from the JAR",
    )

    def to_dict(self) -> dict:
        return {
            "group_id": self.group_id,
            "artifact_id": self.artifact_id,
            "name": self.name,
            "vendor": self.vendor,
            "repository_url": self.repository_url,
            "moment": self.moment,
            "import_names": self.import_names,
        }
