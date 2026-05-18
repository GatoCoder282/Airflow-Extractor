from datetime import datetime, timezone

from extraction.adapters.memory_state_repository import MemoryStateRepository
from extraction.domain.enums import DagRunState, TaskState
from extraction.domain.models import DagRun, TaskInstance
from extraction.services.state_tracker import StateTracker


def _task(state: TaskState) -> TaskInstance:
    return TaskInstance(
        task_id="task_a",
        dag_id="D_BO_0001",
        run_id="scheduled__2026-03-30T00:00:00+00:00",
        state=state,
        start_date=datetime.now(timezone.utc),
        end_date=None,
        duration=None,
        try_number=1,
        max_tries=3,
        operator="BashOperator",
        hostname="worker-1",
        region="BO",
    )


def test_has_changed_detects_new_and_changed_state() -> None:
    repo = MemoryStateRepository()
    tracker = StateTracker(repository=repo)

    first = _task(TaskState.RUNNING)
    assert tracker.has_changed(first) is True
    assert tracker.has_changed(first) is False

    second = _task(TaskState.SUCCESS)
    assert tracker.has_changed(second) is True


def test_has_changed_run_uses_persistent_repo() -> None:
    repo = MemoryStateRepository()
    tracker = StateTracker(repository=repo)

    run = DagRun(
        dag_id="D_BO_0001",
        run_id="run_1",
        state=DagRunState.RUNNING,
        run_type="scheduled",
        start_date=datetime.now(timezone.utc),
        end_date=None,
        execution_date=datetime.now(timezone.utc),
        region="BO",
    )

    assert tracker.has_changed_run(run) is True
    assert tracker.has_changed_run(run) is False
