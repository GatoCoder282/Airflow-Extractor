from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional


class IStateRepository(ABC):
    """Persist the latest known state for task and run keys."""

    @abstractmethod
    def get_last_state(self, region: str, dag_id: str, run_id: str, task_id: str) -> Optional[str]:
        """Return the last known state for a key, if any."""
        ...

    @abstractmethod
    def save_state(
        self,
        region: str,
        dag_id: str,
        run_id: str,
        task_id: str,
        state: str,
        updated_at: datetime,
    ) -> None:
        """Insert or update a state entry."""
        ...

    @abstractmethod
    def get_last_updated_at(self, region: str, dag_id: str) -> Optional[datetime]:
        """Return latest update timestamp for a DAG."""
        ...

    @abstractmethod
    def clear_run(self, region: str, dag_id: str, run_id: str) -> None:
        """Delete cached state for a completed run."""
        ...
