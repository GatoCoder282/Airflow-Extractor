import logging
from typing import List

import psycopg2
from psycopg2.extras import execute_batch

from ..domain.models import DagInfo
from ..ports.catalog_repository import ICatalogRepository

logger = logging.getLogger(__name__)


class PostgresCatalogRepository(ICatalogRepository):
    """PostgreSQL adapter for DAG catalog initialization flow."""

    def __init__(self, host: str, port: int, dbname: str, user: str, password: str):
        self._conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
        )
        self._conn.autocommit = True

    def upsert_dag(self, dag: DagInfo) -> None:
        """Insert or update one DAG catalog row."""
        self.upsert_many_dags([dag])

    def upsert_many_dags(self, dags: List[DagInfo]) -> int:
        """Insert or update many DAG rows preserving business-owned columns."""
        if not dags:
            return 0

        query = (
            "INSERT INTO monitoring.dag_catalog ("
            "dag_id, region, dag_type, dag_subtype, schedule_interval, "
            "timetable_desc, is_active, has_import_error, next_dagrun, "
            "id_file, source_tag, cube_tag, last_seen, created_at, updated_at"
            ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW(), NOW()) "
            "ON CONFLICT (dag_id, region) DO UPDATE SET "
            "dag_type = EXCLUDED.dag_type, "
            "dag_subtype = EXCLUDED.dag_subtype, "
            "schedule_interval = EXCLUDED.schedule_interval, "
            "timetable_desc = EXCLUDED.timetable_desc, "
            "is_active = EXCLUDED.is_active, "
            "has_import_error = EXCLUDED.has_import_error, "
            "next_dagrun = EXCLUDED.next_dagrun, "
            "source_tag = EXCLUDED.source_tag, "
            "cube_tag = EXCLUDED.cube_tag, "
            "last_seen = EXCLUDED.last_seen, "
            "updated_at = NOW()"
        )
        values = [
            (
                dag.dag_id,
                dag.region,
                dag.dag_type.value,
                dag.dag_subtype,
                dag.schedule_interval,
                dag.timetable_desc,
                dag.is_active and not dag.is_paused,
                dag.has_import_error,
                dag.next_dagrun,
                dag.id_file,
                dag.source_tag,
                dag.cube_tag,
            )
            for dag in dags
        ]

        try:
            with self._conn.cursor() as cur:
                execute_batch(cur, query, values, page_size=200)
            return len(dags)
        except psycopg2.Error as e:
            logger.error(f"Error upserting dag_catalog batch: {e}")
            return 0

    def update_sla_seconds(self, dag_id: str, region: str, sla_seconds: int) -> None:
        """Update sla_seconds for a DAG fetched from GET /dags/{dag_id}/details."""
        query = (
            "UPDATE monitoring.dag_catalog "
            "SET sla_seconds = %s, updated_at = NOW() "
            "WHERE dag_id = %s AND region = %s"
        )
        try:
            with self._conn.cursor() as cur:
                cur.execute(query, (sla_seconds, dag_id, region))
        except psycopg2.Error as e:
            logger.error(f"Error updating sla_seconds for dag_id={dag_id}: {e}")

    def refresh_current_status_view(self) -> None:
        """Refresh monitoring.dag_current_status materialized view."""
        query = "REFRESH MATERIALIZED VIEW CONCURRENTLY monitoring.dag_current_status"
        try:
            with self._conn.cursor() as cur:
                cur.execute(query)
        except psycopg2.Error as e:
            logger.error(f"Error refreshing dag_current_status view: {e}")
