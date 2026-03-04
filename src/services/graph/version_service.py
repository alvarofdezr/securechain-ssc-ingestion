from typing import Any

from src.database import DatabaseManager


class VersionService:
    def __init__(self, db: DatabaseManager):
        self.driver = db.get_neo4j_driver()

    async def create_versions(
        self, node_type: str, package_name: str, versions: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        # TODO: Add dynamic labels where Neo4j supports it with indexes
        query = f"""
        MATCH(p:{node_type}{{name:$package_name}})
        WITH p AS package
        UNWIND $versions AS version
        CREATE(v:Version{{
            name: version.name,
            serial_number: version.serial_number,
            mean: version.mean,
            weighted_mean: version.weighted_mean,
            vulnerabilities: version.vulnerabilities,
            release_date: version.release_date
        }})
        CREATE (package)-[:HAVE]->(v)
        RETURN collect({{name: v.name, id: elementid(v)}}) AS versions
        """
        async with self.driver.session() as session:
            result = await session.run(
                query,  # type: ignore
                package_name=package_name,
                versions=versions,
            )
            record = await result.single()
        return record["versions"] if record else []

    async def read_versions_names_by_package(
        self, node_type: str, package_name: str
    ) -> list[str] | None:
        # TODO: Add dynamic labels where Neo4j supports it with indexes
        query = f"""
        MATCH (p:{node_type}{{name:$package_name}})
        MATCH (p)-[:HAVE]->(v)
        RETURN collect(v.name) AS version_names
        """
        async with self.driver.session() as session:
            result = await session.run(query, package_name=package_name)  # type: ignore
            record = await result.single()
        return record["version_names"] if record else None

    async def update_versions_serial_number(
        self,
        node_type: str,
        package_name: str,
        versions: list[dict[str, Any]],
    ) -> None:
        # TODO: Add dynamic labels where Neo4j supports it with indexes
        query = f"""
        MATCH (p:{node_type}{{name: $package_name}})-[:HAVE]->(v)
        WITH v, $versions AS input_versions
        UNWIND input_versions AS version
        WITH v, version
        WHERE v.name = version.name
        SET v.serial_number = version.serial_number
        """
        async with self.driver.session() as session:
            await session.run(
                query,  # type: ignore
                package_name=package_name,
                versions=versions,
            )

    async def count_number_of_versions_by_package(
        self, node_type: str, package_name: str
    ) -> int:
        # TODO: Add dynamic labels where Neo4j supports it with indexes
        query = f"""
        MATCH (p:{node_type}{{name:$package_name}})
        MATCH (p)-[r:HAVE]->(v: Version)
        RETURN count(v) AS version_count
        """
        async with self.driver.session() as session:
            result = await session.run(query, package_name=package_name)  # type: ignore
            record = await result.single()
        return record["version_count"] if record else 0
