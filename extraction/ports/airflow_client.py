from abc import ABC, abstractmethod
from typing import List, Optional, Tuple
from ..domain.models import DagInfo, DagRun, TaskInstance

class IAirflowClient(ABC):
    """
    Contrato para cualquier cliente de Airflow.
    La implementación concreta puede ser HTTP, mock para tests, etc.
    """

    @abstractmethod
    def health_check(self) -> Tuple[bool, str]:
        """Return scheduler health and status text."""
        ...

    @abstractmethod
    def get_all_dags(self) -> List[DagInfo]:
        """Return all DAGs using pagination."""
        ...

    @abstractmethod
    def get_active_dag_runs(self, states: Optional[List[str]] = None) -> List[DagRun]:
        """Return active DAG runs filtered by state."""
        ...

    @abstractmethod
    def get_task_instances(self, dag_run_ids: List[str], states: Optional[List[str]] = None) -> List[TaskInstance]:
        """Return task instances for many run IDs in one call."""
        ...

    @abstractmethod
    def get_import_errors(self) -> List[dict]:
        """Return Airflow import errors."""
        ...

    @abstractmethod
    def get_task_log(self, dag_id: str, run_id: str, task_id: str, try_number: int) -> Optional[str]:
        """Return full task log content for a specific try number."""
        ...

    @abstractmethod
    def get_dag_warnings(self) -> List[dict]:
        """Return Airflow DAG warnings."""
        ...

    @abstractmethod
    def get_dag_details(self, dag_id: str) -> Optional[dict]:
        """Return detailed metadata for a single DAG (includes dag_run_timeout → sla_seconds)."""
        ...

    @abstractmethod
    def get_dag_tasks(self, dag_id: str) -> List[dict]:
        """Return task definitions for a DAG (includes downstream_task_ids for dependency graph)."""
        ...