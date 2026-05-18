import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from ..ports.airflow_client import IAirflowClient
from ..ports.event_publisher import IEventPublisher
from ..domain.models import DagEvent
from ..domain.enums import DagRunState, EventType, TaskState
from .state_tracker import StateTracker

logger = logging.getLogger(__name__)

class ExtractionService:
    """
    Orquesta el ciclo completo de extracción.
    No sabe nada de HTTP, Redis ni Airflow concreto — solo habla con interfaces.
    """

    def __init__(
        self,
        client: IAirflowClient,
        publisher: IEventPublisher,
        tracker: StateTracker,
        region: str = "BO",
        poll_interval_seconds: int = 10,
        task_instances_batch_size: int = 50,
    ):
        """Create an extraction service with polling and state tracking."""
        self._client = client
        self._publisher = publisher
        self._tracker = tracker
        self._region = region
        self._interval = poll_interval_seconds
        self._task_batch_size = max(1, task_instances_batch_size)
        self._running = False
        self._cycle_count = 0
        self._last_complementary_ts = time.monotonic()

    def start(self) -> None:
        """Start the polling loop until a stop signal is received."""
        self._running = True
        logger.info(f"Extractor iniciado — polling cada {self._interval}s")

        while self._running:
            try:
                self._poll_cycle()
            except Exception as e:
                logger.error(f"Error en ciclo {self._cycle_count}: {e}")
            if not self._running:
                break
            time.sleep(self._interval)

    def stop(self) -> None:
        """Stop polling and release publisher resources."""
        self._running = False
        self._publisher.close()
        logger.info(f"Extractor detenido. Ciclos completados: {self._cycle_count}")

    def _poll_cycle(self) -> None:
        self._cycle_count += 1
        logger.debug(f"Ciclo {self._cycle_count} iniciado")

        # 1. Health gate
        is_healthy, scheduler_status = self._client.health_check()
        if not is_healthy:
            self._publish_scheduler_event(scheduler_status)
            self.stop()
            return

        # 2. Fetch runs con cambios recientes (updated_at_gte incremental, sin filtro de estado)
        #    Captura todas las transiciones incluida running→success
        dag_runs = self._client.get_active_dag_runs()
        logger.debug("Runs detectados: %s", len(dag_runs))

        # 3. Categorizar runs por estado
        running_run_ids = [r.run_id for r in dag_runs if r.state == DagRunState.RUNNING]
        failed_run_ids  = [r.run_id for r in dag_runs if r.state == DagRunState.FAILED]

        # 4. Primer paso: detectar qué runs cambiaron de estado (sin API extra)
        changed: dict = {run.run_id: self._tracker.has_changed_run(run) for run in dag_runs}

        # 5. [PRIORIDAD 1] Batch de tasks SOLO para runs terminales que realmente cambiaron
        #    Evita llamadas innecesarias para runs ya procesados en ciclos anteriores
        newly_terminal = [
            r for r in dag_runs
            if changed[r.run_id] and r.state in (DagRunState.SUCCESS, DagRunState.FAILED)
        ]
        terminal_tasks: list = []
        if newly_terminal:
            new_terminal_ids = [r.run_id for r in newly_terminal]
            for batch in self._chunk_run_ids(new_terminal_ids, self._task_batch_size):
                terminal_tasks.extend(self._client.get_task_instances(batch, states=None))
            logger.debug("Conteos de tasks para %s runs terminales nuevos", len(newly_terminal))

        # 5b. Publicar task events de notificación para runs terminales
        #     El extractor normalmente no publica tasks en state=success; este paso
        #     cubre específicamente las tasks que usamos para clasificar el outcome del run.
        _NOTIFICATION_TASKS = frozenset({
            "update_file",
            "notify_success_revision_only",
            "notify_success_download_revision",
            "notify_url_broken",
        })
        for task in terminal_tasks:
            if task.task_id in _NOTIFICATION_TASKS and task.state == TaskState.SUCCESS:
                if self._tracker.has_changed(task):
                    self._publish_task_event(task)

        # 6. Publicar cambios de estado de runs
        for run in dag_runs:
            if changed[run.run_id]:
                if run.state in (DagRunState.SUCCESS, DagRunState.FAILED):
                    counts = self._compute_task_counts(terminal_tasks, run.run_id)
                    self._publish_dag_run_event(run, counts=counts)
                else:
                    self._publish_dag_run_event(run)
            if run.state == DagRunState.SUCCESS:
                # Solo limpiamos en SUCCESS: el run terminó bien y no volverá.
                # En FAILED lo dejamos en el tracker para no re-publicar cada ciclo.
                self._tracker.clear_run(run.region, run.dag_id, run.run_id)

        # 7. [PRIORIDAD 2+3] Tasks activas para runs en RUNNING
        #    running/queued → task actual en ejecución; skipped → patrón short-circuit
        if running_run_ids:
            for batch in self._chunk_run_ids(running_run_ids, self._task_batch_size):
                active_tasks = self._client.get_task_instances(
                    batch, states=["running", "queued", "skipped"]
                )
                logger.debug("Tasks activas detectadas: %s", len(active_tasks))
                for task in active_tasks:
                    if self._tracker.has_changed(task):
                        self._publish_task_event(task)

        # 8. [PRIORIDAD 3] Tasks de runs FAILED — incluye skipped para detectar short-circuits
        if failed_run_ids:
            for batch in self._chunk_run_ids(failed_run_ids, self._task_batch_size):
                tasks = self._client.get_task_instances(
                    batch,
                    states=["failed", "upstream_failed", "up_for_retry", "skipped"],
                )
                logger.debug("Tasks fallidas detectadas: %s", len(tasks))
                for task in tasks:
                    if self._tracker.has_changed(task):
                        self._publish_task_event(task)
                    self._maybe_publish_task_log(task)

        # 9. Checks complementarios cada 300s (sin cambios)
        now = time.monotonic()
        if now - self._last_complementary_ts >= 300:
            self._last_complementary_ts = now
            self._check_import_errors()
            self._check_dag_warnings()

    def _publish_scheduler_event(self, scheduler_status: str) -> None:
        event = DagEvent(
            event_type=EventType.SCHEDULER_UNHEALTHY,
            dag_id="__scheduler__",
            region="global",
            run_id=None,
            run_state=scheduler_status,
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
        self._publisher.publish(event)
        logger.info("Scheduler unhealthy: %s", scheduler_status)

    def _publish_dag_run_event(self, run, counts: dict | None = None) -> None:
        c = counts or {}
        event = DagEvent(
            event_type=EventType.DAG_RUN_STATE_CHANGE,
            dag_id=run.dag_id,
            region=run.region,
            run_id=run.run_id,
            run_state=run.state.value,
            run_type=run.run_type,
            execution_date=run.execution_date,
            start_date=run.start_date,
            end_date=run.end_date,
            duration=None,
            task_id=None,
            task_state=None,
            try_number=None,
            max_tries=None,
            total_tasks=c.get("total_tasks"),
            success_tasks=c.get("success_tasks"),
            failed_tasks=c.get("failed_tasks"),
            skipped_tasks=c.get("skipped_tasks"),
        )
        self._publisher.publish(event)
        logger.debug(
            "DAG run state change: dag_id=%s run_id=%s state=%s counts=%s",
            run.dag_id, run.run_id, run.state.value, c or "sin-counts",
        )

    def _publish_task_event(self, task) -> None:
        event = DagEvent(
            event_type=EventType.TASK_STATE_CHANGE,
            dag_id=task.dag_id,
            region=task.region,
            run_id=task.run_id,
            run_state=None,
            run_type=None,
            execution_date=None,
            start_date=task.start_date,
            end_date=task.end_date,
            duration=task.duration,
            task_id=task.task_id,
            task_state=task.state.value,
            try_number=task.try_number,
            max_tries=task.max_tries,
            sla_miss=task.sla_miss,
        )
        self._publisher.publish(event)
        logger.debug(
            "Task state change: dag_id=%s run_id=%s task_id=%s state=%s",
            task.dag_id,
            task.run_id,
            task.task_id,
            task.state.value,
        )

    def _publish_task_log_event(self, task, log_text: str) -> None:
        event = DagEvent(
            event_type=EventType.TASK_LOG,
            dag_id=task.dag_id,
            region=task.region,
            run_id=task.run_id,
            run_state=None,
            run_type=None,
            execution_date=None,
            start_date=task.start_date,
            end_date=task.end_date,
            duration=task.duration,
            task_id=task.task_id,
            task_state=task.state.value,
            try_number=task.try_number,
            max_tries=task.max_tries,
            sla_miss=task.sla_miss,
            detail=log_text,
        )
        self._publisher.publish(event)
        logger.debug(
            "Task log captured: dag_id=%s run_id=%s task_id=%s try=%s",
            task.dag_id,
            task.run_id,
            task.task_id,
            task.try_number,
        )

    def _maybe_publish_task_log(self, task) -> None:
        if task.state not in (TaskState.FAILED, TaskState.UPSTREAM_FAILED, TaskState.UP_FOR_RETRY):
            return
        if task.try_number < 1:
            return

        log_task_id = f"__log__:{task.task_id}:{task.try_number}"
        already_sent = self._tracker._repo.get_last_state(
            task.region, task.dag_id, task.run_id, log_task_id
        )
        if already_sent:
            return

        log_text = self._client.get_task_log(task.dag_id, task.run_id, task.task_id, task.try_number)
        if not log_text:
            return

        self._tracker._repo.save_state(
            task.region, task.dag_id, task.run_id, log_task_id,
            "sent", datetime.now(timezone.utc),
        )
        self._publish_task_log_event(task, log_text)

    def _check_import_errors(self) -> None:
        for import_error in self._client.get_import_errors():
            filename = import_error.get("filename", "")
            dag_id = Path(filename).stem if filename else "__unknown__"
            detail = (
                import_error.get("stack_trace")
                or import_error.get("error")
                or import_error.get("message")
            )
            event = DagEvent(
                event_type=EventType.IMPORT_ERROR,
                dag_id=dag_id,
                region=self._region,
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
                detail=detail,
            )
            self._publisher.publish(event)
            logger.debug("Import error detected: dag_id=%s", dag_id)

    def _check_dag_warnings(self) -> None:
        for warning in self._client.get_dag_warnings():
            dag_id = warning.get("dag_id", "__unknown__")
            detail = warning.get("message") or warning.get("warning")
            event = DagEvent(
                event_type=EventType.DAG_WARNING,
                dag_id=dag_id,
                region=self._region,
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
                detail=detail,
            )
            self._publisher.publish(event)
            logger.debug("DAG warning detected: dag_id=%s", dag_id)

    @staticmethod
    def _chunk_run_ids(run_ids: list[str], batch_size: int) -> list[list[str]]:
        return [run_ids[i : i + batch_size] for i in range(0, len(run_ids), batch_size)]

    @staticmethod
    def _compute_task_counts(tasks: list, run_id: str) -> dict:
        """Filtra tasks de un run específico y retorna conteos por estado.
        upstream_failed se suma en failed_tasks porque representa el mismo punto de fallo.
        """
        run_tasks = [t for t in tasks if t.run_id == run_id]
        return {
            "total_tasks":   len(run_tasks),
            "success_tasks": sum(1 for t in run_tasks if t.state == TaskState.SUCCESS),
            "failed_tasks":  sum(1 for t in run_tasks if t.state in (
                                 TaskState.FAILED, TaskState.UPSTREAM_FAILED)),
            "skipped_tasks": sum(1 for t in run_tasks if t.state == TaskState.SKIPPED),
        }