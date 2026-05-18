import logging
import signal
import sys

from .config import ExtractorConfig
from ..infrastructure.factory import ExtractorFactory

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)

logger = logging.getLogger(__name__)


def main() -> None:
    """Bootstrap extractor initialization and polling services."""
    config = ExtractorConfig.from_env()
    logger.info(f"Iniciando extractor - region={config.region}")

    init_service = ExtractorFactory.create_initialization_service(config)
    init_result = init_service.run()
    if not init_result.success:
        logger.error("Inicializacion fallida - abortando")
        sys.exit(1)

    logger.info(f"Inicializacion exitosa: {init_result.total_dags} DAGs procesados")
    service = ExtractorFactory.create_extraction_service(config)

    def handle_shutdown(sig, frame) -> None:
        logger.info(f"Senal {sig} recibida - iniciando shutdown graceful")
        service.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    service.start()

if __name__ == "__main__":
    main()