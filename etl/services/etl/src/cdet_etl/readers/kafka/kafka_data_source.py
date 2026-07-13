from dataclasses import dataclass

from cdet_etl.readers.base_data_source import BaseDataSource

@dataclass(frozen=True, kw_only=True)
class KafkaDataSource(BaseDataSource):
    """Immutable type container for real-time Apache Kafka event topics configuration."""
    bootstrap_servers: str = "kafka:9092"
    topic: str = "telemetry_stream"
    group_id: str = "cdet_etl_workers"
    max_batch_size: int = 5000

    @property
    def lineage_token(self) -> str:
        return f"kafka://{self.bootstrap_servers}/{self.topic}/{self.group_id}"
