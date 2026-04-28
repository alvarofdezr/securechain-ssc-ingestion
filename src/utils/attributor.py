from typing import Any


class Attributor:
    """Attributes vulnerability data and impact metrics to package versions.
    
    Aggregates severity information from detected vulnerabilities and computes
    statistical impact metrics (arithmetic mean and weighted mean) used for
    risk assessment and vulnerability prioritization in the dependency graph.
    
    The weighted mean metric emphasizes higher-severity vulnerabilities, making it
    suitable for risk-based ordering and alerting strategies.
    
    Attributes:
        vulnerability_service: Service for querying vulnerability data by package and version.
        impacts: List of impact scores (CVSS v3.0 base scores) collected during
            the current attribution operation. Cleared before each attribution.
    """

    def __init__(
        self,
        vulnerability_service: Any,
    ):
        """Initializes the attributor with a vulnerability query service.
        
        Args:
            vulnerability_service: Service instance providing async methods to query
                vulnerabilities by package name and version identifier.
        """
        self.vulnerability_service = vulnerability_service
        self.impacts: list[float] = []

    async def attribute_vulnerabilities(
        self, package_name: str, version: Any
    ) -> dict[str, Any]:
        """Fetches and attributes vulnerability data to a package version.
        
        Queries the vulnerability service for all known vulnerabilities affecting
        the specified package version. For each vulnerability, extracts CVSS v3.0
        base scores and computes aggregate impact metrics. Augments the version
        dictionary with vulnerability IDs, arithmetic mean, and weighted mean.
        
        The version dictionary is modified in-place and returned for convenience.
        
        Args:
            package_name: Package identifier (e.g., 'requests', 'lodash').
            version: Version dictionary with required 'name' key (e.g., {'name': '1.0.0'}).
        
        Returns:
            The same version dictionary, augmented with:
            - vulnerabilities: List of vulnerability IDs affecting this version.
            - mean: Arithmetic mean of CVSS v3.0 base scores (0.0 if no vulns).
            - weighted_mean: Weighted mean emphasizing high-severity vulns (0.0 if no vulns).
        
        Raises:
            Exception: Propagates exceptions from the vulnerability service.
        """
        self.impacts = []

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
        """Computes the arithmetic mean of collected impact scores.
        
        Returns:
            Arithmetic mean of CVSS v3.0 base scores, rounded to 2 decimal places.
            Returns 0.0 if no impact scores have been collected.
        """
        if self.impacts:
            return round(sum(self.impacts) / len(self.impacts), 2)
        return 0.0

    def weighted_mean(self) -> float:
        """Computes a weighted mean emphasizing higher-severity vulnerabilities.
        
        Applies a weighting strategy that emphasizes higher base scores: each impact
        score is squared and scaled by 0.1 in the numerator, while being scaled by
        0.1 in the denominator. This produces a mean biased toward the higher values
        in the distribution.
        
        Formula: sum(impact^2 * 0.1) / sum(impact * 0.1)
        
        Returns:
            Weighted mean of CVSS v3.0 base scores, rounded to 2 decimal places.
            Returns 0.0 if no impact scores have been collected.
        """
        if self.impacts:
            return round(
                sum(var**2 * 0.1 for var in self.impacts)
                / sum(var * 0.1 for var in self.impacts),
                2,
            )
        return 0.0
