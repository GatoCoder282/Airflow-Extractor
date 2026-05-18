import logging
import time
from dataclasses import dataclass

from ..domain.enums import EventType
from ..domain.models import DagEvent
from ..ports.airflow_client import IAirflowClient
from ..ports.catalog_repository import ICatalogRepository
from ..ports.event_publisher import IEventPublisher

logger = logging.getLogger(__name__)


@dataclass
class InitializationResult:
    total_dags: int
    upserted: int
    with_errors: int
    import_errors: int
    duration_ms: float
    success: bool


class InitializationService:
    """Run the one-time startup DAG catalog synchronization flow."""

    def __init__(self, client: IAirflowClient, catalog_repo: ICatalogRepository, publisher: IEventPublisher):
        """Initialize service dependencies for startup sync."""
        self._client = client
        self._catalog = catalog_repo
        self._publisher = publisher

    def run(self) -> InitializationResult:
        """Execute initialization flow and return run metrics."""
        start = time.perf_counter()

        is_healthy, scheduler_status = self._client.health_check()
        if not is_healthy:
            raise RuntimeError(f"Scheduler unhealthy: {scheduler_status}")

        dags = self._client.get_all_dags()
        upserted = self._catalog.upsert_many_dags(dags)

        # Second pass: enrich each DAG with sla_seconds from /dags/{dag_id}/details
        # TEMP: solo DAGs no pausados para reducir llamadas a la API durante pruebas
        active_dags = [dag for dag in dags if not dag.is_paused]
        logger.info("SLA enrichment: %s DAGs activos de %s totales", len(active_dags), len(dags))
        sla_updated = 0
        for dag in active_dags:
            details = self._client.get_dag_details(dag.dag_id)
            if details:
                timeout_raw = details.get("dag_run_timeout")
                if timeout_raw is not None:
                    try:
                        sla_seconds = int(float(timeout_raw))
                        self._catalog.update_sla_seconds(dag.dag_id, dag.region, sla_seconds)
                        sla_updated += 1
                    except (TypeError, ValueError):
                        pass
            # Small delay to avoid hammering the Airflow API
            time.sleep(0.05)

        logger.info("SLA enrichment complete: %s/%s DAGs activos actualizados con sla_seconds", sla_updated, len(active_dags))

        self._catalog.refresh_current_status_view()

        import_errors = self._client.get_import_errors()
        self._publisher.publish(
            DagEvent(
                event_type=EventType.DAG_CATALOG_SYNC,
                dag_id="__catalog__",
                region="global",
                run_id=None,
                run_state=None,
                run_type=None,
                execution_date=None,
                start_date=None,
                end_date=None,
                duration=None,
                task_id=None,
                task_state=None,
                try_number=None,
                max_tries=None,
            )
        )

        duration_ms = (time.perf_counter() - start) * 1000
        result = InitializationResult(
            total_dags=len(dags),
            upserted=upserted,
            with_errors=sum(1 for dag in dags if dag.has_import_error),
            import_errors=len(import_errors),
            duration_ms=duration_ms,
            success=upserted == len(dags),
        )
        logger.info(
            "Initialization completed: total=%s upserted=%s import_errors=%s duration_ms=%.2f",
            result.total_dags,
            result.upserted,
            result.import_errors,
            result.duration_ms,
        )
        return result
