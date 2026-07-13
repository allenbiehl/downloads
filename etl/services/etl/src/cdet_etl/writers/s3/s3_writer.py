from cdet_etl.writers.base_metadata_provider import BaseMetadataProvider
from cdet_etl.writers.base_writer import BaseWriter
from cdet_etl.writers.s3.s3_write_context import S3WriteContext

class S3Writer(BaseWriter):
    """Public Parameterized S3 Transactional Ingest Writer via Composition."""
    def __init__(self, properties: dict, metadata_provider: BaseMetadataProvider = None):
        super().__init__(context=S3WriteContext(
            properties=properties,
            metadata_provider=metadata_provider
        ))
