import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

@dataclass
class ExtractorConfig:
    airflow_url: str
    airflow_user: str
    airflow_password: str
    redis_host: str
    redis_port: int
    redis_password: Optional[str]
    redis_db: int
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    region: str
    poll_interval: int
    publisher_type: str
    state_repository_type: str
    airflow_request_timeout_seconds: int
    task_instances_batch_size: int
    dag_runs_lookback_days: int

    @classmethod
    def from_env(cls) -> "ExtractorConfig":
        return cls(
            airflow_url = os.environ["AIRFLOW_URL"],
            airflow_user = os.environ["AIRFLOW_USER"],
            airflow_password = os.environ["AIRFLOW_PASSWORD"],
            redis_host = os.getenv("REDIS_HOST", "localhost"),
            redis_port = int(os.getenv("REDIS_PORT", "6379")),
            redis_password = os.getenv("REDIS_PASSWORD"),
            redis_db = int(os.getenv("REDIS_DB", "0")),
            postgres_host = os.environ["POSTGRES_HOST"],
            postgres_port = int(os.getenv("POSTGRES_PORT", "5432")),
            postgres_db = os.environ["POSTGRES_DB"],
            postgres_user = os.environ["POSTGRES_USER"],
            postgres_password = os.environ["POSTGRES_PASSWORD"],
            region = os.getenv("REGION", "BO"),
            poll_interval = int(os.getenv("POLL_INTERVAL_SECONDS", "10")),
            publisher_type = os.getenv("PUBLISHER_TYPE", "stdout").lower(),
            state_repository_type = os.getenv("STATE_REPOSITORY_TYPE", "memory").lower(),
            airflow_request_timeout_seconds = int(os.getenv("AIRFLOW_REQUEST_TIMEOUT_SECONDS", "30")),
            task_instances_batch_size = int(os.getenv("TASK_INSTANCES_BATCH_SIZE", "50")),
            dag_runs_lookback_days = int(os.getenv("DAG_RUNS_LOOKBACK_DAYS", "1")),
        )