from typing import Any

from univers.versions import (
    MavenVersion,
    NugetVersion,
    PypiVersion,
    RubygemsVersion,
    SemverVersion,
    Version,
)


class Orderer:
    def __init__(
        self,
        node_type: str,
    ):
        self.node_type = node_type

    def order_versions(self, versions: list[dict[str, str]]) -> list[dict]:
        version_type: Version = self.get_version_type()
        final_versions: list[dict] = []
        univers_versions: list[tuple[Version, dict[str, Any]]] = []
        for version in versions:
            try:
                univers_versions.append((version_type(version.get("name")), version))  # type: ignore[call-arg]
            except Exception:
                final_versions.append(
                    {"name": version.get("name"), "serial_number": -1}
                )
        univers_versions.sort(key=lambda pair: pair[0])
        for serial_number, (_, raw_version) in enumerate(univers_versions):
            final_versions.append(
                {
                    "name": raw_version.get("name"),
                    "release_date": raw_version.get("release_date"),
                    "serial_number": serial_number,
                }
            )
        return final_versions

    def get_version_type(self) -> Version:
        return {
            "PyPIPackage": PypiVersion,
            "NPMPackage": SemverVersion,
            "CargoPackage": SemverVersion,
            "MavenPackage": MavenVersion,
            "RubyGemsPackage": RubygemsVersion,
            "NuGetPackage": NugetVersion,
            "GoPackage": SemverVersion,  # added for Go, using Semver as a common choice for Go versioning
        }.get(self.node_type, Version)
