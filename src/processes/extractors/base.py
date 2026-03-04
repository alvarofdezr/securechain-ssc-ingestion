from abc import ABC, abstractmethod


class PackageExtractor(ABC):
    def __init__(
        self,
        constraints: str | None = None,
        parent_id: str | None = None,
        parent_version_name: str | None = None,
        refresh: bool = False,
    ):
        self.constraints = constraints
        self.parent_id = parent_id
        self.parent_version_name = parent_version_name
        self.refresh = refresh

    @abstractmethod
    async def run(self) -> None: ...
