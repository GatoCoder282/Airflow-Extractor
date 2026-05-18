from datetime import datetime, timezone
from typing import List, Optional

from extraction.adapters.memory_state_repository import MemoryStateRepository
from extraction.domain.enums import DagRunState, TaskState
from extraction.domain.models import DagRun, TaskInstance
from extraction.ports.airflow_client import IAirflowClient
from extraction.ports.event_publisher import IEventPublisher
from extraction.services.extraction_service import ExtractionService
from extraction.services.state_tracker import StateTracker


class FakeAirflowClient(IAirflowClient):
    def __init__(self):
        self._runs = [
            DagRun(
                dag_id="D_BO_0001",
                run_id="run_1",
                state=DagRunState.FAILED,
                run_type="scheduled",
                start_date=datetime.now(timezone.utc),
                end_date=None,
                execution_date=datetime.now(timezone.utc),
                region="BO",
            )
        ]

    def health_check(self) -> tuple[bool, str]:
        return True, "healthy"

    def get_all_dags(self) -> list:
        return []

    def get_active_dag_runs(self, states: Optional[List[str]] = None) -> List[DagRun]:
        return self._runs

    def get_task_instances(self, dag_run_ids: List[str], states: Optional[List[str]] = None) -> List[TaskInstance]:
        return [
            TaskInstance(
                task_id="task_a",
                dag_id="D_BO_0001",
                run_id="run_1",
                state=TaskState.FAILED,
                start_date=datetime.now(timezone.utc),
                end_date=None,
                duration=3.0,
                try_number=1,
                max_tries=3,
                operator="BashOperator",
                hostname="worker-1",
                region="BO",
            )
        ]

    def get_import_errors(self) -> list[dict]:
        return []

    def get_task_log(self, dag_id: str, run_id: str, task_id: str, try_number: int) -> Optional[str]:
        return None

    def get_dag_warnings(self) -> list[dict]:
        return []


class CollectingPublisher(IEventPublisher):
    def __init__(self):
        self.events = []

    def publish(self, event) -> str:
        self.events.append(event)
        return "1-0"

    def close(self) -> None:
        return None


def test_poll_cycle_publishes_run_and_task_changes() -> None:
    client = FakeAirflowClient()
    publisher = CollectingPublisher()
    tracker = StateTracker(repository=MemoryStateRepository())

    service = ExtractionService(
        client=client,
        publisher=publisher,
        tracker=tracker,
        poll_interval_seconds=1,
    )

    service._poll_cycle()

    event_types = [event.event_type.value for event in publisher.events]
    assert "dag_run_state_change" in event_types
    assert "task_state_change" in event_types
