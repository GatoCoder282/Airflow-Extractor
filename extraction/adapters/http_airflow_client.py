import re
import requests
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from requests.auth import HTTPBasicAuth

_SOURCE_RE = re.compile(r'^[A-Za-z]+$')

from ..ports.airflow_client import IAirflowClient
from ..domain.models import DagInfo, DagRun, TaskInstance
from ..domain.enums import DagRunState, DagType, TaskState

logger = logging.getLogger(__name__)

class HttpAirflowClient(IAirflowClient):
    """HTTP adapter for Airflow REST API 2.7.2."""

    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        region: str = "BO",
        timeout_seconds: int = 10,
        lookback_days: int = 1,
    ):
        """Initialize the HTTP client for Airflow REST API."""
        self._base = f"{base_url.rstrip('/')}/api/v1"
        self._auth = HTTPBasicAuth(username, password)
        self._region = region
        self._session = requests.Session()
        self._session.auth = self._auth
        self._timeout = timeout_seconds
        self._lookback_days = lookback_days
        logger.info("[%s] Ventana de extracción: últimos %s día(s)", region, lookback_days)

    def health_check(self) -> tuple[bool, str]:
        """Return scheduler health flag and status text."""
        try:
            response = self._session.get(f"{self._base}/health", timeout=self._timeout)
            response.raise_for_status()
            payload = response.json()
            scheduler_status = payload.get("scheduler", {}).get("status", "unknown")
            return scheduler_status == "healthy", scheduler_status
        except requests.RequestException as e:
            logger.error(f"[{self._region}] Health check failed: {e}")
            return False, "unreachable"

    def get_all_dags(self) -> List[DagInfo]:
        """Return all DAGs using pagination."""
        dags: List[DagInfo] = []
        limit = 100
        offset = 0

        while True:
            try:
                response = self._session.get(
                    f"{self._base}/dags",
                    params={"limit": limit, "offset": offset},  
                    timeout=self._timeout,
                )
                response.raise_for_status()
                payload = response.json()
                items = payload.get("dags", [])
                dags.extend(self._parse_dag_info(item) for item in items)

                total_entries = payload.get("total_entries", len(dags))
                offset += limit
                if offset >= total_entries:
                    break
            except requests.RequestException as e:
                logger.error(f"[{self._region}] Error fetching dags page offset={offset}: {e}")
                return []

        return dags

    def get_active_dag_runs(self, states: Optional[List[str]] = None) -> List[DagRun]:
        """Return DAG runs filtered by state within the lookback window.

        Incluye 'success' para capturar la transición running→success.
        El state tracker previene re-publicar eventos ya enviados.
        """
        selected_states = states or ["running", "queued", "failed", "success"]
        runs: List[DagRun] = []
        page_offset = 0
        page_limit = 200
        start_date_gte = (datetime.now(timezone.utc) - timedelta(days=self._lookback_days)).isoformat()

        while True:
            body = {
                "states": selected_states,
                "page_limit": page_limit,
                "page_offset": page_offset,
                "order_by": "-start_date",
                "start_date_gte": start_date_gte,
            }
            try:
                response = self._session.post(
                    f"{self._base}/dags/~/dagRuns/list",
                    json=body,
                    timeout=self._timeout,
                )
                response.raise_for_status()
                payload = response.json()
                items = payload.get("dag_runs", [])
                runs.extend(self._parse_dag_run(item) for item in items)

                total_entries = payload.get("total_entries", len(runs))
                page_offset += page_limit
                if page_offset >= total_entries or not items:
                    break
            except requests.RequestException as e:
                logger.error(f"[{self._region}] Error fetching dag runs: {e}")
                return []

        return runs

    def get_task_instances(self, dag_run_ids: List[str], states: Optional[List[str]] = None) -> List[TaskInstance]:
        """Return task instances for many run IDs in one call.
        Pass states=None to retrieve ALL states (used for task count computation at run completion).
        """
        if not dag_run_ids:
            return []

        body: dict = {"dag_run_ids": dag_run_ids}
        if states is not None:  # None explícito = sin filtro, Airflow retorna todos los estados
            body["state"] = states

        try:
            response = self._session.post(
                f"{self._base}/dags/~/dagRuns/~/taskInstances/list",
                json=body,
                timeout=self._timeout,
            )
            response.raise_for_status()
            return [self._parse_task(item) for item in response.json().get("task_instances", [])]
        except requests.RequestException as e:
            logger.error(f"[{self._region}] Error fetching task instances: {e}")
            return []

    def get_import_errors(self) -> List[dict]:
        """Return Airflow import errors."""
        try:
            response = self._session.get(f"{self._base}/importErrors", timeout=self._timeout)
            response.raise_for_status()
            return response.json().get("import_errors", [])
        except requests.RequestException as e:
            logger.error(f"[{self._region}] Error fetching import errors: {e}")
            return []

    def get_task_log(self, dag_id: str, run_id: str, task_id: str, try_number: int) -> Optional[str]:
        """Return full task log content for a specific try number."""
        try:
            response = self._session.get(
                f"{self._base}/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/{try_number}",
                params={"full_content": "true"},
                timeout=self._timeout,
            )
            response.raise_for_status()
            content_type = (response.headers.get("Content-Type") or "").lower()
            if "application/json" in content_type:
                payload = response.json()
                content = payload.get("content")
                return content if isinstance(content, str) else None
            return response.text
        except requests.RequestException as e:
            logger.error(
                f"[{self._region}] Error fetching task log dag_id={dag_id} run_id={run_id} task_id={task_id}: {e}"
            )
            return None

    def get_dag_warnings(self) -> List[dict]:
        """Return Airflow DAG warnings."""
        try:
            response = self._session.get(f"{self._base}/dagWarnings", timeout=self._timeout)
            response.raise_for_status()
            return response.json().get("dag_warnings", [])
        except requests.RequestException as e:
            logger.error(f"[{self._region}] Error fetching dag warnings: {e}")
            return []

    def get_dag_details(self, dag_id: str) -> Optional[dict]:
        """Return detailed metadata for a DAG including dag_run_timeout → sla_seconds."""
        try:
            response = self._session.get(
                f"{self._base}/dags/{dag_id}/details",
                timeout=self._timeout,
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"[{self._region}] Error fetching dag details dag_id={dag_id}: {e}")
            return None

    def get_dag_tasks(self, dag_id: str) -> List[dict]:
        """Return task definitions for a DAG including downstream_task_ids."""
        try:
            response = self._session.get(
                f"{self._base}/dags/{dag_id}/tasks",
                timeout=self._timeout,
            )
            response.raise_for_status()
            return response.json().get("tasks", [])
        except requests.RequestException as e:
            logger.error(f"[{self._region}] Error fetching dag tasks dag_id={dag_id}: {e}")
            return []

    def _parse_dag_info(self, data: dict) -> DagInfo:
        dag_id = data.get("dag_id", "")
        source_tag, cube_tag = self._classify_tags(data.get("tags", []))
        return DagInfo(
            dag_id=dag_id,
            dag_type=self._infer_dag_type(dag_id),
            dag_subtype=self._infer_dag_subtype(data.get("description"), data.get("fileloc")),
            is_active=bool(data.get("is_active", False)),
            is_paused=bool(data.get("is_paused", False)),
            has_import_error=bool(data.get("has_import_errors", False)),
            schedule_interval=data.get("schedule_interval", {}).get("value") if isinstance(data.get("schedule_interval"), dict) else data.get("schedule_interval"),
            timetable_desc=data.get("timetable_description"),
            next_dagrun=self._parse_dt(data.get("next_dagrun")),
            description=data.get("description"),
            fileloc=data.get("fileloc"),
            id_file=None,
            source_tag=source_tag,
            cube_tag=cube_tag,
            region=self._region,
        )

    @staticmethod
    def _classify_tags(tags: list) -> tuple:
        source, cube = None, None
        for t in tags:
            name = t.get("name", "")
            if _SOURCE_RE.match(name):
                source = name
            else:
                cube = name
        return source, cube

    def _parse_dag_run(self, data: dict) -> DagRun:
        raw_state = (data.get("state") or "queued").lower()
        try:
            state = DagRunState(raw_state)
        except ValueError:
            state = DagRunState.QUEUED

        return DagRun(
            dag_id=data["dag_id"],
            run_id=data.get("dag_run_id") or data.get("run_id"),
            state=state,
            run_type=data.get("run_type", "manual"),
            start_date=self._parse_dt(data.get("start_date")),
            end_date=self._parse_dt(data.get("end_date")),
            execution_date=self._parse_dt(data.get("execution_date") or data.get("logical_date")),
            region=self._region,
        )

    def _parse_task(self, data: dict) -> TaskInstance:
        raw_state = (data.get("state") or "queued").lower()
        try:
            state = TaskState(raw_state)
        except ValueError:
            state = TaskState.QUEUED

        return TaskInstance(
            task_id=data["task_id"],
            dag_id=data["dag_id"],
            run_id=data["dag_run_id"],
            state=state,
            start_date=self._parse_dt(data.get("start_date")),
            end_date=self._parse_dt(data.get("end_date")),
            duration=data.get("duration"),
            try_number=int(data.get("try_number", 1)),
            max_tries=int(data.get("max_tries", 0)),
            operator=data.get("operator"),
            hostname=data.get("hostname"),
            sla_miss=bool(data.get("sla_miss", False)),
            region=self._region,
        )

    @staticmethod
    def _infer_dag_type(dag_id: str) -> DagType:
        prefix = dag_id.split("_")[0].upper() if dag_id else ""
        mapping = {
            "C": DagType.CONVERSION,
            "D": DagType.DESCARGA,
            "M": DagType.MIGRACION,
            "DB": DagType.DATABASE,
        }
        return mapping.get(prefix, DagType.UNKNOWN)

    @staticmethod
    def _infer_dag_subtype(description: Optional[str], fileloc: Optional[str]) -> Optional[str]:
        source = f"{description or ''} {fileloc or ''}".lower()
        if any(token in source for token in ["csv", "json", "pdf", "url"]):
            return "D1"
        if "visualiz" in source:
            return "D2"
        if any(token in source for token in ["scrap", "html"]):
            return "D3"
        if any(token in source for token in ["masiv", "bulk"]):
            return "D4"
        return None

    @staticmethod
    def _parse_dt(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        return datetime.fromisoformat(value.replace("Z", "+00:00"))