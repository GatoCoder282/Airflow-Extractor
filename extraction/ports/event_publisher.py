from abc import ABC, abstractmethod
from typing import Optional
from ..domain.models import DagEvent

class IEventPublisher(ABC):
    """
    Contrato para publicar eventos.
    Puede ser Redis, Kafka, stdout, o cualquier otro destino.
    """

    @abstractmethod
    def publish(self, event: DagEvent) -> Optional[str]:
        """Publish event and return broker message id when available."""
        ...

    @abstractmethod
    def close(self) -> None:
        """Close publisher resources gracefully."""
        ...