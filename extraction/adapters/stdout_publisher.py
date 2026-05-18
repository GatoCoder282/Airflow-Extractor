import json
import logging
from typing import Optional

from ..ports.event_publisher import IEventPublisher
from ..domain.models import DagEvent

logger = logging.getLogger(__name__)

class StdoutPublisher(IEventPublisher):
    """
    Publica eventos en stdout.
    Usado en desarrollo — sin necesidad de Redis ni Kafka.
    Reemplazar por RedisPublisher o KafkaPublisher en producción.
    """

    def publish(self, event: DagEvent) -> Optional[str]:
        """Log event payload as JSON and return a local pseudo id."""
        payload = event.to_redis_dict()
        logger.info(json.dumps(payload, ensure_ascii=True))
        return f"local-{event.timestamp.isoformat()}"

    def close(self) -> None:
        """No-op for stdout transport."""
        return None