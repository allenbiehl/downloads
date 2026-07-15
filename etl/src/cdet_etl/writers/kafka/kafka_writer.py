from cdet_etl.writers.base_metadata_provider import BaseMetadataProvider
from cdet_etl.writers.base_writer import BaseWriter
from cdet_etl.writers.kafka.kafka_write_context import KafkaWriteContext

class KafkaWriter(BaseWriter):
    """Public Parameterized kafka Transactional Ingest Writer via Composition."""
    def __init__(self, properties: dict | None = None, metadata_provider: BaseMetadataProvider | None = None):
        super().__init__(context=KafkaWriteContext(
            properties=properties,
            metadata_provider=metadata_provider
        ))
