from abc import ABC, abstractmethod

class BaseReader(ABC):
    """
    Template Base Class for S3/MinIO Ingestors.
    Exclusively utilizes PyArrow's C++ S3FileSystem for unified network pooling.
    """

    @abstractmethod
    def can_handle(self, s3_path: str) -> bool:
        """Enforces format compliance evaluation across the Chain of Responsibility."""

    @abstractmethod
    def stream_chunks(self, s3_path: str):
        """
        Standardized Data Engineering Stream Provider.
        Reused by all native formats; overridden exclusively by complex types like XML.
        """
