from abc import ABC, abstractmethod

class BaseConfigRepository(ABC):
    """
    Pure Agnostic Interface for configuration backends.
    Encapsulates lifecycle lookups and hot-reload detection rules.
    """

    @abstractmethod
    def is_modified(self, dataflow_id: str) -> bool:
        """Evaluates whether the underlying configuration source has changed."""

    @abstractmethod
    def fetch_blueprint(self, dataflow_id: str) -> dict:
        """Retrieves and parses the specific dataflow dictionary blueprint matrix."""
