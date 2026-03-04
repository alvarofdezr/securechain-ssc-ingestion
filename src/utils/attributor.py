from typing import Any


class Attributor:
    def __init__(
        self,
        vulnerability_service: Any,
    ):
        self.vulnerability_service = vulnerability_service
        self.impacts: list[float] = []

    async def attribute_vulnerabilities(
        self, package_name: str, version: Any
    ) -> dict[str, Any]:
        vulnerabilities = await self.vulnerability_service.read_vulnerabilities_by_package_and_version(
            package_name, version["name"]
        )
        version["vulnerabilities"] = []
        for vuln in vulnerabilities:
            version["vulnerabilities"].append(vuln["id"])
            if "severity" in vuln:
                for severity in vuln["severity"]:
                    if severity["type"] == "CVSS_V3":
                        self.impacts.append(severity["base_score"])
        version["mean"] = self.mean()
        version["weighted_mean"] = self.weighted_mean()
        return version

    def mean(self) -> float:
        if self.impacts:
            return round(sum(self.impacts) / len(self.impacts), 2)
        return 0.0

    def weighted_mean(self) -> float:
        if self.impacts:
            return round(
                sum(var**2 * 0.1 for var in self.impacts)
                / sum(var * 0.1 for var in self.impacts),
                2,
            )
        return 0.0
