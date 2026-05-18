from unittest.mock import Mock
from datetime import datetime, timezone

import redis

from extraction.adapters.redis_publisher import RedisPublisher
from extraction.domain.enums import EventType
from extraction.domain.models import DagEvent


def test_redis_publisher_xadd() -> None:
    """Valida que RedisPublisher publique en el stream correcto."""
    mock_redis = Mock()
    mock_redis.xadd.return_value = "1234567890-0"
    
    publisher = RedisPublisher("localhost", 6379)
    publisher._redis = mock_redis  # inyectar mock
    
    event = DagEvent(
        event_type=EventType.TASK_STATE_CHANGE,
        dag_id="D_BO_0001",
        region="BO",
        run_id="run_1",
        run_state=None,
        run_type=None,
        execution_date=None,
        start_date=datetime.now(timezone.utc),
        end_date=None,
        duration=5.0,
        task_id="task_1",
        task_state="success",
        try_number=1,
        max_tries=3,
    )
    
    msg_id = publisher.publish(event)
    
    # Verifica que XADD fue llamado correctamente
    mock_redis.xadd.assert_called_once()
    call_args = mock_redis.xadd.call_args
    assert call_args.kwargs["name"] == "stream:airflow_events"
    assert msg_id == "1234567890-0"


def test_redis_publisher_handles_connection_error() -> None:
    """Valida error handling cuando Redis no está disponible."""
    mock_redis = Mock()
    mock_redis.xadd.side_effect = redis.RedisError("Connection refused")
    
    publisher = RedisPublisher("localhost", 6379)
    publisher._redis = mock_redis
    
    event = DagEvent(
        event_type=EventType.SCHEDULER_UNHEALTHY,
        dag_id="__scheduler__",
        region="global",
        run_id=None,
        run_state="unhealthy",
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
    
    msg_id = publisher.publish(event)
    assert msg_id is None  # No falla, retorna None


def test_redis_publisher_dag_catalog_sync_stream() -> None:
    """Valida que DAG_CATALOG_SYNC use el stream correcto."""
    mock_redis = Mock()
    mock_redis.xadd.return_value = "9876543210-0"
    
    publisher = RedisPublisher("localhost", 6379)
    publisher._redis = mock_redis
    
    event = DagEvent(
        event_type=EventType.DAG_CATALOG_SYNC,
        dag_id="D_BO_0001",
        region="BO",
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
    
    msg_id = publisher.publish(event)
    
    # Verifica que use el stream correcto para DAG_CATALOG_SYNC
    call_args = mock_redis.xadd.call_args
    assert call_args.kwargs["name"] == "stream:dag_catalog_sync"
    assert msg_id == "9876543210-0"


def test_redis_publisher_respects_maxlen() -> None:
    """Valida que se respete el límite máximo de mensajes en stream."""
    mock_redis = Mock()
    mock_redis.xadd.return_value = "5555555555-0"
    
    publisher = RedisPublisher("localhost", 6379)
    publisher._redis = mock_redis
    
    event = DagEvent(
        event_type=EventType.TASK_STATE_CHANGE,
        dag_id="D_BO_0001",
        region="BO",
        run_id="run_1",
        run_state=None,
        run_type=None,
        execution_date=None,
        start_date=datetime.now(timezone.utc),
        end_date=None,
        duration=None,
        task_id="task_1",
        task_state="running",
        try_number=1,
        max_tries=3,
    )
    
    publisher.publish(event)
    
    # Verifica que se pasó el maxlen correcto
    call_args = mock_redis.xadd.call_args
    assert call_args.kwargs["maxlen"] == 50_000
    assert call_args.kwargs["approximate"] is True


def test_redis_publisher_converts_event_to_dict() -> None:
    """Valida que el evento se convierta correctamente a diccionario para Redis."""
    mock_redis = Mock()
    mock_redis.xadd.return_value = "1111111111-0"
    
    publisher = RedisPublisher("localhost", 6379)
    publisher._redis = mock_redis
    
    event = DagEvent(
        event_type=EventType.IMPORT_ERROR,
        dag_id="D_BO_0002",
        region="BO",
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
        sla_miss=False,
    )
    
    publisher.publish(event)
    
    # Verifica el contenido del diccionario publicado
    call_args = mock_redis.xadd.call_args
    fields = call_args.kwargs["fields"]
    
    assert fields["event_type"] == "import_error_detected"
    assert fields["dag_id"] == "D_BO_0002"
    assert fields["region"] == "BO"
    assert fields["sla_miss"] == "false"
    assert "timestamp" in fields
