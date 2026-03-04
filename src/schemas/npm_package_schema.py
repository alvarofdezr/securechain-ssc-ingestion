from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class NPMPackageSchema(BaseModel):
    model_config = ConfigDict(validate_assignment=True, str_strip_whitespace=True)

    name: str = Field(..., description="Name of the NPM package")
    vendor: str = Field(default="n/a", description="Vendor source")
    repository_url: str = Field(
        default="n/a", description="Repository URL (GitHub/GitLab/Bitbucket)"
    )
    moment: datetime = Field(
        default_factory=datetime.now, description="Timestamp of the last update"
    )
    import_names: list[str] = Field(
        default_factory=list,
        description="List of module paths extracted from the NPM package",
    )

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "vendor": self.vendor,
            "repository_url": self.repository_url,
            "moment": self.moment,
            "import_names": self.import_names,
        }
