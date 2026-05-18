from abc import ABC, abstractmethod
from typing import List

from ..domain.models import DagInfo


class ICatalogRepository(ABC):
    """Write DAG catalog information during initialization flow."""

    @abstractmethod
    def upsert_dag(self, dag: DagInfo) -> None:
        """Insert or update one DAG in catalog."""
        ...

    @abstractmethod
    def upsert_many_dags(self, dags: List[DagInfo]) -> int:
        """Insert or update many DAGs in batch."""
        ...

    @abstractmethod
    def refresh_current_status_view(self) -> None:
        """Refresh materialized DAG status view."""
        ...

    @abstractmethod
    def update_sla_seconds(self, dag_id: str, region: str, sla_seconds: int) -> None:
        """Update sla_seconds for a DAG in the catalog."""
        ...
