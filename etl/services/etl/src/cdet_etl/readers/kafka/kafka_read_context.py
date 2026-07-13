# cdet_etl/readers/kafka/kafka_read_context.py
import json
import pandas as pd
from kafka import KafkaConsumer
from cdet_etl.readers.base_read_context import BaseReadContext

class KafkaReadContext(BaseReadContext):
    """
    Concrete Streaming Context for Apache Kafka.
    Runs an open-ended message polling loop to feed the data pipeline in real time.
    """
    def __init__(self, properties: dict):
        super().__init__(properties)
        self._bootstrap_servers = properties.get("bootstrap_servers", "localhost:9092")
        self._topic = properties.get("topic", "telemetry_stream")
        self._group_id = properties.get("group_id", "cdet_etl_workers")
        self._consumer = None

    @property
    def _stream_registry(self) -> dict:
        return {} # Streaming brokers do not utilize extension-based format chains

    def establish_connection(self):
        """Initializes the long-lived Kafka consumer network connection client."""
        # Dynamic import prevents crashing container runtimes that do not require Kafka libraries
    
        
        if self._consumer is None:
            self._consumer = KafkaConsumer(
                self._topic,
                bootstrap_servers=self._bootstrap_servers,
                group_id=self._group_id,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8"))
            )
            print(f"[KAFKA CLIENT] Connected to topic '{self._topic}' at {self._bootstrap_servers}")

    def read_stream_listener(self):
        """
        Polls Kafka continuously, bundles incoming events into mini-batches,
        and yields standard DataFrames down the transformation chain.
        """
        batch_buffer = []
        max_batch_size = 5000  # Micro-batch row threshold to maintain flat memory ceilings

        # Continuous message broker event poll loop
        for message in self._consumer:
            batch_buffer.append(message.value)

            if len(batch_buffer) >= max_batch_size:
                chunk_df = pd.DataFrame(batch_buffer)
                batch_buffer.clear()
                
                if not chunk_df.empty:
                    yield chunk_df
