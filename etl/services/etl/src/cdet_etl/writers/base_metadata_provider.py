from abc import ABC, abstractmethod


class BaseMetadataProvider(ABC):
    """Abstract strategy interface for S3 object metadata enrichment."""

    def __init__(self, properties: dict = None):
        self._properties = properties or {}

    @abstractmethod
    def get_upload_kwargs(self, *, metadata: dict | None = None) -> dict:
        """Generates compliant boto3 create_multipart_upload keyword arguments."""
