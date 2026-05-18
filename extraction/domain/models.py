from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
from .enums import DagRunState, DagType, EventType, TaskState

@dataclass(frozen=True)
class TaskInstance:
    task_id:    str
    dag_id:     str
    run_id:     str
    state:      TaskState
    start_date: Optional[datetime]
    end_date:   Optional[datetime]
    duration:   Optional[float]
    try_number: int
    max_tries:  int
    operator:   Optional[str]
    hostname:   Optional[str]
    sla_miss:   bool = False
    region:     str = "BO"  # preparado para multi-país

@dataclass(frozen=True)
class DagRun:
    dag_id:     str
    run_id:     str
    state:      DagRunState
    run_type:   str
    start_date: Optional[datetime]
    end_date:   Optional[datetime]
    execution_date: Optional[datetime]
    region:     str = "BO"


@dataclass(frozen=True)
class DagInfo:
    dag_id: str
    dag_type: DagType
    dag_subtype: Optional[str]
    is_active: bool
    is_paused: bool
    has_import_error: bool
    schedule_interval: Optional[str]
    timetable_desc: Optional[str]
    next_dagrun: Optional[datetime]
    description: Optional[str]
    fileloc: Optional[str]
    id_file: Optional[int]
    source_tag: Optional[str] = None
    cube_tag: Optional[str] = None
    region: str = "BO"

@dataclass(frozen=True)
class DagEvent:
    event_type: EventType
    dag_id: str
    region: str

    run_id: Optional[str]
    run_state: Optional[str]
    run_type: Optional[str]
    execution_date: Optional[datetime]
    start_date: Optional[datetime]
    end_date: Optional[datetime]
    duration: Optional[float]

    task_id: Optional[str]
    task_state: Optional[str]
    try_number: Optional[int]
    max_tries: Optional[int]
    sla_miss: bool = False
    detail: Optional[str] = None
    total_tasks:   Optional[int] = None
    success_tasks: Optional[int] = None
    failed_tasks:  Optional[int] = None
    skipped_tasks: Optional[int] = None

    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_redis_dict(self) -> dict[str, str]:
        """Convert event to flat string payload for Redis Streams."""
        data: dict[str, str] = {
            "event_type": self.event_type.value,
            "dag_id": self.dag_id,
            "region": self.region,
            "timestamp": self.timestamp.isoformat(),
            "sla_miss": "true" if self.sla_miss else "false",
        }

        optional_fields: dict[str, Optional[str]] = {
            "run_id": self.run_id,
            "run_state": self.run_state,
            "run_type": self.run_type,
            "execution_date": self.execution_date.isoformat() if self.execution_date else None,
            "start_date": self.start_date.isoformat() if self.start_date else None,
            "end_date": self.end_date.isoformat() if self.end_date else None,
            "duration": str(self.duration) if self.duration is not None else None,
            "task_id": self.task_id,
            "task_state": self.task_state,
            "try_number": str(self.try_number) if self.try_number is not None else None,
            "max_tries": str(self.max_tries) if self.max_tries is not None else None,
            "detail": self.detail,
            "total_tasks":   str(self.total_tasks)   if self.total_tasks   is not None else None,
            "success_tasks": str(self.success_tasks) if self.success_tasks is not None else None,
            "failed_tasks":  str(self.failed_tasks)  if self.failed_tasks  is not None else None,
            "skipped_tasks": str(self.skipped_tasks) if self.skipped_tasks is not None else None,
        }
        data.update({key: value for key, value in optional_fields.items() if value is not None})
        return data