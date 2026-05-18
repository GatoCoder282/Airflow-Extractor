import logging
from datetime import datetime
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor

from ..ports.state_repository import IStateRepository

logger = logging.getLogger(__name__)


class SqlStateRepository(IStateRepository):
    """PostgreSQL-backed state repository using upsert semantics."""

    def __init__(self, host: str, port: int, dbname: str, user: str, password: str):
        self._conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
        )
        self._conn.autocommit = True

    def get_last_state(self, region: str, dag_id: str, run_id: str, task_id: str) -> Optional[str]:
        """Return latest known state for task key from extractor_state."""
        query = (
            "SELECT last_state "
            "FROM monitoring.extractor_state "
            "WHERE region = %s AND dag_id = %s AND run_id = %s AND task_id = %s"
        )
        try:
            with self._conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, (region, dag_id, run_id, task_id))
                row = cur.fetchone()
                return row["last_state"] if row else None
        except psycopg2.Error as e:
            logger.error(f"Error reading state from PostgreSQL: {e}")
            return None

    def save_state(
        self,
        region: str,
        dag_id: str,
        run_id: str,
        task_id: str,
        state: str,
        updated_at: datetime,
    ) -> None:
        """Upsert latest state for task key into extractor_state."""
        query = (
            "INSERT INTO monitoring.extractor_state "
            "(region, dag_id, run_id, task_id, last_state, updated_at) "
            "VALUES (%s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (region, dag_id, run_id, task_id) "
            "DO UPDATE SET last_state = EXCLUDED.last_state, updated_at = EXCLUDED.updated_at"
        )
        try:
            with self._conn.cursor() as cur:
                cur.execute(query, (region, dag_id, run_id, task_id, state, updated_at))
        except psycopg2.Error as e:
            logger.error(f"Error writing state to PostgreSQL: {e}")

    def get_last_updated_at(self, region: str, dag_id: str) -> Optional[datetime]:
        """Return last update timestamp for any key in the given DAG."""
        query = (
            "SELECT MAX(updated_at) AS last_updated_at "
            "FROM monitoring.extractor_state "
            "WHERE region = %s AND dag_id = %s"
        )
        try:
            with self._conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, (region, dag_id))
                row = cur.fetchone()
                return row["last_updated_at"] if row else None
        except psycopg2.Error as e:
            logger.error(f"Error reading last_updated_at from PostgreSQL: {e}")
            return None

    def clear_run(self, region: str, dag_id: str, run_id: str) -> None:
        """Delete all task states for a completed run."""
        query = (
            "DELETE FROM monitoring.extractor_state "
            "WHERE region = %s AND dag_id = %s AND run_id = %s"
        )
        try:
            with self._conn.cursor() as cur:
                cur.execute(query, (region, dag_id, run_id))
        except psycopg2.Error as e:
            logger.error(f"Error clearing run state from PostgreSQL: {e}")
