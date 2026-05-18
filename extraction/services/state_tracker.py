from datetime import datetime, timezone
from typing import Dict

from ..domain.models import DagRun, TaskInstance
from ..domain.enums import TaskState, SemaphoreColor
from ..ports.state_repository import IStateRepository

class StateTracker:
    """
    Detecta cambios de estado entre polling cycles.
    Solo emite un evento cuando el estado de una task cambia,
    no en cada consulta.
    """

    def __init__(self, repository: IStateRepository):
        """Initialize tracker with a persistent state repository."""
        self._repo = repository
        self._cache: Dict[str, str] = {}

    def has_changed(self, task: TaskInstance) -> bool:
        key = self._key(task.region, task.dag_id, task.run_id, task.task_id)
        current = task.state.value

        cached = self._cache.get(key)
        if cached == current:
            return False

        if cached is None:
            persisted = self._repo.get_last_state(task.region, task.dag_id, task.run_id, task.task_id)
            if persisted == current:
                self._cache[key] = current
                return False

        self._cache[key] = current
        self._repo.save_state(
            task.region,
            task.dag_id,
            task.run_id,
            task.task_id,
            current,
            datetime.now(timezone.utc),
        )
        return True

    def has_changed_run(self, run: DagRun) -> bool:
        """Return True only when a DAG run state changes."""
        synthetic_task_id = f"__run__:{run.run_id}"
        key = self._key(run.region, run.dag_id, run.run_id, synthetic_task_id)
        current = run.state.value

        cached = self._cache.get(key)
        if cached == current:
            return False

        if cached is None:
            persisted = self._repo.get_last_state(run.region, run.dag_id, run.run_id, synthetic_task_id)
            if persisted == current:
                self._cache[key] = current
                return False

        self._cache[key] = current
        self._repo.save_state(
            run.region,
            run.dag_id,
            run.run_id,
            synthetic_task_id,
            current,
            datetime.now(timezone.utc),
        )
        return True

    def calculate_semaphore(self, task: TaskInstance) -> SemaphoreColor:
        if task.state in (TaskState.FAILED, TaskState.UPSTREAM_FAILED):
            return SemaphoreColor.RED
        if task.try_number > 1:
            return SemaphoreColor.RED
        if task.sla_miss:
            return SemaphoreColor.RED
        if task.state in (TaskState.RUNNING, TaskState.UP_FOR_RETRY):
            return SemaphoreColor.YELLOW
        if task.state == TaskState.SUCCESS:
            return SemaphoreColor.GREEN
        return SemaphoreColor.YELLOW

    def clear_run(self, region: str, dag_id: str, run_id: str) -> None:
        """Remove run keys from memory and persistent repository."""
        prefix = self._key(region, dag_id, run_id, "")
        keys_to_delete = [key for key in self._cache if key.startswith(prefix)]
        for key in keys_to_delete:
            del self._cache[key]
        self._repo.clear_run(region, dag_id, run_id)

    @staticmethod
    def _key(region: str, dag_id: str, run_id: str, task_id: str) -> str:
        return f"{region}:{dag_id}:{run_id}:{task_id}"