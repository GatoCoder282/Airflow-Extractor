from typing import Any

from extraction.adapters.http_airflow_client import HttpAirflowClient
from extraction.domain.enums import DagRunState, DagType, TaskState


class _FakeResponse:
    def __init__(self, payload: dict[str, Any], raise_error: bool = False):
        self._payload = payload
        self._raise_error = raise_error

    def raise_for_status(self) -> None:
        if self._raise_error:
            raise RuntimeError("http error")

    def json(self) -> dict[str, Any]:
        return self._payload


class _FakeSession:
    def __init__(self, get_responses: list[_FakeResponse], post_responses: list[_FakeResponse]):
        self.get_responses = get_responses
        self.post_responses = post_responses
        self.get_calls: list[dict[str, Any]] = []
        self.post_calls: list[dict[str, Any]] = []
        self.auth = None

    def get(self, url: str, **kwargs):
        self.get_calls.append({"url": url, **kwargs})
        return self.get_responses.pop(0)

    def post(self, url: str, **kwargs):
        self.post_calls.append({"url": url, **kwargs})
        return self.post_responses.pop(0)


def test_get_all_dags_paginates_and_parses() -> None:
    client = HttpAirflowClient("http://airflow", "user", "pass", region="BO")
    fake_session = _FakeSession(
        get_responses=[
            _FakeResponse(
                {
                    "total_entries": 101,
                    "dags": [
                        {
                            "dag_id": "D_BO_0001",
                            "is_active": True,
                            "is_paused": False,
                            "has_import_errors": False,
                            "description": "descarga csv diaria",
                            "fileloc": "/dags/download/job.py",
                        }
                    ],
                }
            ),
            _FakeResponse(
                {
                    "total_entries": 101,
                    "dags": [
                        {
                            "dag_id": "DB_BO_0002",
                            "is_active": True,
                            "is_paused": False,
                            "has_import_errors": True,
                            "description": "db sync",
                            "fileloc": "/dags/db/sync.py",
                        }
                    ],
                }
            ),
        ],
        post_responses=[],
    )
    client._session = fake_session

    dags = client.get_all_dags()

    assert len(dags) == 2
    assert dags[0].dag_type == DagType.DESCARGA
    assert dags[0].dag_subtype == "D1"
    assert dags[1].dag_type == DagType.DATABASE
    assert fake_session.get_calls[0]["params"]["offset"] == 0
    assert fake_session.get_calls[1]["params"]["offset"] == 100


def test_get_active_dag_runs_uses_batch_endpoint_with_pagination() -> None:
    client = HttpAirflowClient("http://airflow", "user", "pass", region="BO")
    fake_session = _FakeSession(
        get_responses=[],
        post_responses=[
            _FakeResponse(
                {
                    "total_entries": 201,
                    "dag_runs": [
                        {
                            "dag_id": "D_BO_0001",
                            "dag_run_id": "run_1",
                            "state": "running",
                            "run_type": "scheduled",
                            "logical_date": "2026-03-30T00:00:00Z",
                        }
                    ],
                }
            ),
            _FakeResponse(
                {
                    "total_entries": 201,
                    "dag_runs": [
                        {
                            "dag_id": "D_BO_0002",
                            "dag_run_id": "run_2",
                            "state": "failed",
                            "run_type": "manual",
                            "logical_date": "2026-03-30T01:00:00Z",
                        }
                    ],
                }
            ),
        ],
    )
    client._session = fake_session

    runs = client.get_active_dag_runs()

    assert len(runs) == 2
    assert runs[0].state == DagRunState.RUNNING
    assert runs[1].state == DagRunState.FAILED
    assert fake_session.post_calls[0]["url"].endswith("/dags/~/dagRuns/list")
    assert fake_session.post_calls[0]["json"]["page_offset"] == 0
    assert fake_session.post_calls[1]["json"]["page_offset"] == 200


def test_get_task_instances_uses_batch_run_ids() -> None:
    client = HttpAirflowClient("http://airflow", "user", "pass", region="BO")
    fake_session = _FakeSession(
        get_responses=[],
        post_responses=[
            _FakeResponse(
                {
                    "task_instances": [
                        {
                            "task_id": "task_a",
                            "dag_id": "D_BO_0001",
                            "dag_run_id": "run_1",
                            "state": "up_for_retry",
                            "try_number": 2,
                            "max_tries": 3,
                            "sla_miss": True,
                        }
                    ]
                }
            )
        ],
    )
    client._session = fake_session

    tasks = client.get_task_instances(["run_1", "run_2"])

    assert len(tasks) == 1
    assert tasks[0].state == TaskState.UP_FOR_RETRY
    assert tasks[0].try_number == 2
    assert fake_session.post_calls[0]["url"].endswith("/dags/~/dagRuns/~/taskInstances/list")
    assert fake_session.post_calls[0]["json"]["dag_run_ids"] == ["run_1", "run_2"]
