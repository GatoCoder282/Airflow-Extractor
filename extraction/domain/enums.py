from enum import Enum

class TaskState(str, Enum):
    SUCCESS  = "success"
    FAILED   = "failed"
    RUNNING  = "running"
    QUEUED   = "queued"
    SKIPPED  = "skipped"
    UPSTREAM_FAILED = "upstream_failed"
    UP_FOR_RETRY = "up_for_retry"
    DEFERRED = "deferred"
    REMOVED = "removed"
    RESTARTING = "restarting"
    SCHEDULED = "scheduled"
    UP_FOR_RESCHEDULE = "up_for_reschedule"
    NO_STATUS = "no_status"


class DagRunState(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"

class SemaphoreColor(str, Enum):
    GREEN  = "green"   # todo ok
    YELLOW = "yellow"  # advertencia — corriendo más de lo normal
    RED    = "red"     # fallo crítico


class DagType(str, Enum):
    CONVERSION = "C"
    DESCARGA = "D"
    MIGRACION = "M"
    DATABASE = "DB"
    UNKNOWN = "?"


class EventType(str, Enum):
    TASK_STATE_CHANGE = "task_state_change"
    DAG_RUN_STATE_CHANGE = "dag_run_state_change"
    IMPORT_ERROR = "import_error_detected"
    SCHEDULER_UNHEALTHY = "scheduler_unhealthy"
    DAG_CATALOG_SYNC = "dag_catalog_sync"
    TASK_LOG = "task_log"
    DAG_WARNING = "dag_warning"


class IncidentCategory(str, Enum):
    URL_DEAD = "url_dead"
    REPORT_NOT_GENERATED = "report_not_generated"
    DOWNLOAD_DELAY = "download_delay"
    RETRY_EXCEEDED = "retry_exceeded"
    STRUCTURE_CHANGE = "structure_change"
    SCHEDULER_ISSUE = "scheduler_issue"
    IMPORT_ERROR = "import_error"