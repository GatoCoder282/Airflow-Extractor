import logging
from typing import Optional

import redis

from ..domain.enums import EventType
from ..domain.models import DagEvent
from ..ports.event_publisher import IEventPublisher

logger = logging.getLogger(__name__)


class RedisPublisher(IEventPublisher):
	"""Publish extractor events to Redis Streams."""

	STREAMS = {
		EventType.TASK_STATE_CHANGE: "stream:airflow_events",
		EventType.DAG_RUN_STATE_CHANGE: "stream:airflow_events",
		EventType.IMPORT_ERROR: "stream:airflow_events",
		EventType.SCHEDULER_UNHEALTHY: "stream:airflow_events",
		EventType.TASK_LOG: "stream:airflow_events",
EventType.DAG_WARNING: "stream:airflow_events",
		EventType.DAG_CATALOG_SYNC: "stream:dag_catalog_sync",
	}

	MAX_LEN = {
		"stream:airflow_events": 50_000,
		"stream:dag_catalog_sync": 1_000,
	}

	def __init__(self, host: str, port: int, db: int = 0, password: Optional[str] = None):
		"""Initialize Redis connection for publishing events."""
		self._redis = redis.Redis(host=host, port=port, db=db, password=password, decode_responses=True)

	def publish(self, event: DagEvent) -> Optional[str]:
		"""Publish event and return Redis message ID, or None on failure."""
		try:
			stream = self.STREAMS[event.event_type]
			msg_id = self._redis.xadd(
				name=stream,
				fields=event.to_redis_dict(),
				maxlen=self.MAX_LEN.get(stream, 10_000),
				approximate=True,
			)
			return msg_id
		except redis.RedisError as e:
			logger.error(f"Redis publish failed: {e}")
			return None

	def close(self) -> None:
		"""Close redis connection pool."""
		try:
			self._redis.close()
		except redis.RedisError as e:
			logger.warning(f"Redis close failed: {e}")
