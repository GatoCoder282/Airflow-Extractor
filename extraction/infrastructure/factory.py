from ..extractor.config import ExtractorConfig
from ..adapters.http_airflow_client import HttpAirflowClient
from ..adapters.memory_state_repository import MemoryStateRepository
from ..adapters.postgres_catalog_repository import PostgresCatalogRepository
from ..adapters.redis_publisher import RedisPublisher
from ..adapters.sql_state_repository import SqlStateRepository
from ..adapters.stdout_publisher import StdoutPublisher
from ..ports.event_publisher import IEventPublisher
from ..ports.state_repository import IStateRepository
from ..services.initialization_service import InitializationService
from ..services.state_tracker import StateTracker
from ..services.extraction_service import ExtractionService

class ExtractorFactory:
    """Compose all services and adapters for extractor runtime."""

    @staticmethod
    def create_extraction_service(config: ExtractorConfig) -> ExtractionService:
        """Build ExtractionService with configured adapters."""
        client = HttpAirflowClient(
            base_url=config.airflow_url,
            username=config.airflow_user,
            password=config.airflow_password,
            region=config.region,
            timeout_seconds=config.airflow_request_timeout_seconds,
            lookback_days=config.dag_runs_lookback_days,
        )
        publisher = ExtractorFactory._create_publisher(config)
        state_repo = ExtractorFactory._create_state_repository(config)
        tracker = StateTracker(repository=state_repo)

        return ExtractionService(
            client=client,
            publisher=publisher,
            tracker=tracker,
            region=config.region,
            poll_interval_seconds=config.poll_interval,
            task_instances_batch_size=config.task_instances_batch_size,
        )

    @staticmethod
    def create_initialization_service(config: ExtractorConfig) -> InitializationService:
        """Build InitializationService with catalog repository and publisher."""
        client = HttpAirflowClient(
            base_url=config.airflow_url,
            username=config.airflow_user,
            password=config.airflow_password,
            region=config.region,
            timeout_seconds=config.airflow_request_timeout_seconds,
            lookback_days=config.dag_runs_lookback_days,
        )
        catalog = PostgresCatalogRepository(
            host=config.postgres_host,
            port=config.postgres_port,
            dbname=config.postgres_db,
            user=config.postgres_user,
            password=config.postgres_password,
        )
        publisher = ExtractorFactory._create_publisher(config)
        return InitializationService(client=client, catalog_repo=catalog, publisher=publisher)

    @staticmethod
    def _create_publisher(config: ExtractorConfig) -> IEventPublisher:
        if config.publisher_type == "redis":
            return RedisPublisher(
                host=config.redis_host,
                port=config.redis_port,
                db=config.redis_db,
                password=config.redis_password,
            )
        return StdoutPublisher()

    @staticmethod
    def _create_state_repository(config: ExtractorConfig) -> IStateRepository:
        if config.state_repository_type == "sql":
            return SqlStateRepository(
                host=config.postgres_host,
                port=config.postgres_port,
                dbname=config.postgres_db,
                user=config.postgres_user,
                password=config.postgres_password,
            )
        return MemoryStateRepository()