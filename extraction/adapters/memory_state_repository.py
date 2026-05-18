from datetime import datetime
from typing import Dict, Optional, Tuple

from ..ports.state_repository import IStateRepository


class MemoryStateRepository(IStateRepository):
    """In-memory state repository for tests and local development."""

    def __init__(self):
        self._states: Dict[Tuple[str, str, str, str], Tuple[str, datetime]] = {}

    def get_last_state(self, region: str, dag_id: str, run_id: str, task_id: str) -> Optional[str]:
        """Return state value for key if it exists."""
        value = self._states.get((region, dag_id, run_id, task_id))
        return value[0] if value else None

    def save_state(
        self,
        region: str,
        dag_id: str,
        run_id: str,
        task_id: str,
        state: str,
        updated_at: datetime,
    ) -> None:
        """Insert or update state value in memory."""
        self._states[(region, dag_id, run_id, task_id)] = (state, updated_at)

    def get_last_updated_at(self, region: str, dag_id: str) -> Optional[datetime]:
        """Return max update timestamp for all tasks in a DAG."""
        timestamps = [
            updated_at
            for (reg, d_id, _run_id, _task_id), (_state, updated_at) in self._states.items()
            if reg == region and d_id == dag_id
        ]
        return max(timestamps) if timestamps else None

    def clear_run(self, region: str, dag_id: str, run_id: str) -> None:
        """Delete all state keys for a finished run."""
        to_delete = [
            key
            for key in self._states
            if key[0] == region and key[1] == dag_id and key[2] == run_id
        ]
        for key in to_delete:
            del self._states[key]
