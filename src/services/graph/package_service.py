from collections.abc import AsyncGenerator
from datetime import datetime
from typing import Any

from src.database import DatabaseManager


class PackageService:
    def __init__(self, db: DatabaseManager):
        self.driver = db.get_neo4j_driver()

    async def create_package_and_versions(
        self,
        node_type: str,
        package: dict[str, Any],
        versions: list[dict[str, Any]],
        constraints: str | None = None,
        parent_id: str | None = None,
        parent_version_name: str | None = None,
    ) -> list[dict[str, str]]:
        # TODO: Add dynamic labels where Neo4j supports it with indexes
        parent_match = ""
        parent_rel = ""
        if parent_id:
            parent_match = """
                MATCH (parent:RequirementFile|Version)
                WHERE elementid(parent) = $parent_id
                """
            parent_rel = f"CREATE (parent)-[rel_p:REQUIRE{{constraints:$constraints{', parent_version_name:$parent_version_name' if parent_version_name else ''}}}]->(p)"
        pkg_key = (
            "group_id:$group_id, artifact_id:$artifact_id, name:$name"
            if node_type == "MavenPackage"
            else "name:$name"
        )

        query = f"""
        {parent_match}
        MERGE(p:{node_type}{{{pkg_key}}})
        ON CREATE SET p.vendor = $vendor, p.moment = $moment, p.repository_url = $repository_url, p.import_names = $import_names
        ON MATCH SET p.moment = $moment
        {parent_rel}
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
                package,
                constraints=constraints,
                parent_id=parent_id,
                parent_version_name=parent_version_name,
                versions=versions,
            )
            record = await result.single()
        return record["versions"] if record else []

    async def read_package_by_name(
        self, node_type: str, package_name: str
    ) -> dict[str, Any]:
        # TODO: Add dynamic labels where Neo4j supports it with indexes
        query = f"""
        MATCH(p:{node_type}{{name:$package_name}})
        RETURN p{{id: elementid(p), .*}} AS package
        """
        async with self.driver.session() as session:
            result = await session.run(
                query,  # type: ignore
                package_name=package_name,
            )
            record = await result.single()
        return record["package"] if record else {}

    async def read_packages_in_batches(
        self, node_type: str, batch_size: int = 1000
    ) -> AsyncGenerator[list[dict]]:
        skip = 0
        while True:
            # TODO: Add dynamic labels where Neo4j supports it with indexes
            query = f"""
            MATCH (p:{node_type})
            RETURN p.name AS name, p.moment AS moment
            SKIP $skip LIMIT $limit
            """
            async with self.driver.session() as session:
                result = await session.run(query, skip=skip, limit=batch_size)  # type: ignore
                records = [record async for record in result]
                if not records:
                    break
                yield [r.data() for r in records]
            skip += batch_size

    async def relate_packages(
        self, node_type: str, packages: list[dict[str, Any]]
    ) -> None:
        # TODO: Add dynamic labels where Neo4j supports it with indexes
        query = f"""
        UNWIND $packages AS package
        MATCH (parent:RequirementFile|Version)
        WHERE elementid(parent) = package.parent_id
        MATCH (p:{node_type})
        WHERE elementid(p) = package.id
        CREATE (parent)-[:REQUIRE{{constraints: package.constraints, parent_version_name: package.parent_version_name}}]->(p)
        """
        async with self.driver.session() as session:
            await session.run(query, packages=packages)  # type: ignore

    async def update_package_moment(self, node_type: str, package_name: str) -> None:
        # TODO: Add dynamic labels where Neo4j supports it with indexes
        query = f"""
        MATCH(p:{node_type}{{name:$package_name}})
        SET p.moment = $moment
        """
        async with self.driver.session() as session:
            await session.run(query, package_name=package_name, moment=datetime.now())  # type: ignore
