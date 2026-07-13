from cdet_etl.writers.base_metadata_provider import BaseMetadataProvider

class DefaultMetadataProvider(BaseMetadataProvider):
    """
    Highly Flexible Production Metadata Strategy.
    Natively supports both standard AWS parameters and static custom metadata headers.
    """
    def __init__(self, properties: dict = None):
        super().__init__(properties)
        self._static_headers = self._properties.get("static_headers", {})
        self._custom_metadata = self._properties.get("custom_metadata", {})

    def get_upload_kwargs(self, *, metadata: dict | None = None) -> dict:
        metadata_payload = metadata or {}

        for meta_key, meta_value in self._custom_metadata.items():
            metadata_payload[meta_key.lower()] = str(meta_value)

        upload_kwargs = {
            "Metadata": metadata_payload,
        }

        for header_key, header_value in self._static_headers.items():
            upload_kwargs[header_key] = header_value

        return upload_kwargs
