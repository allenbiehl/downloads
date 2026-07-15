from abc import ABC, abstractmethod

class BaseDataSource(ABC):
    """
    Pure Abstract Base for all pipeline entry channels.
    Provides the master type hint variable anchor for the reader hierarchy.
    """
    @property
    @abstractmethod
    def metadata(self) -> dict:
        """Returns the raw, open-ended configuration mapping dict for tracking and lineage."""
