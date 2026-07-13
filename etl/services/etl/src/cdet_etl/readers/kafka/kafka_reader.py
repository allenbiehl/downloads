import json
import pandas as pd
from cdet_etl.readers.base_data_source import BaseDataSource
from cdet_etl.readers.base_reader import BaseReader
from cdet_etl.readers.kafka.kafka_data_source import KafkaDataSource
from cdet_etl.readers.kafka.kafka_client_factory import KafkaClientFactory

import time
import pandas as pd
from cdet_etl.core.sources import DataflowSource, KafkaStreamSource
from cdet_etl.readers.base_reader_framework import BaseReader
from cdet_etl.infrastructure.kafka_client_factory import KafkaClientFactory


# cdet_etl/readers/kafka/kafka_reader.py
import pandas as pd
from cdet_etl.core.sources import DataflowSource, KafkaStreamSource
from cdet_etl.readers.base_reader_framework import BaseReader
from cdet_etl.infrastructure.kafka_client_factory import KafkaClientFactory


class KafkaReader(BaseReader):
    """
    Performance-Optimized Kafka Consumer.
    
    Uses native C++ blocking socket timeouts to flush mini-batches 
    without high-CPU loop spinning or manual time tracking calculations.
    """

    def __init__(self, properties: dict):
        self._properties = properties
        
        # 1. Compile the maximum allowed data wait boundary directly in the constructor
        max_wait_seconds = float(properties.get("max_wait_seconds", 1.0))
        
        # 2. Scale the native poll timeout to match the wait threshold exactly (e.g., 1.0s -> 1000ms)
        self._poll_timeout_ms = int(max_wait_seconds * 1000)

    def read_stream(self, source: DataflowSource):
        if not isinstance(source, KafkaDataSource):
            raise TypeError(f"KafkaReader requires a KafkaStreamSource instance, received: '{type(source).__name__}'")

        # Local cache pointers allocated to eliminate object property lookups inside the loop
        max_batch_size = source.max_batch_size
        poll_timeout_ms = self._poll_timeout_ms

        consumer = KafkaClientFactory.create_consumer(source=source, properties=self._properties)
        print(f"[KAFKA NATIVE LOOP] Polling topic '{source.topic}' with blocking timeout: {poll_timeout_ms}ms")

        batch_buffer = []

        try:
            while True:
                # The poll blocks cleanly at the socket layer until messages arrive OR the timeout expires.
                # This guarantees near-zero CPU usage when the topic is completely silent.
                message_pack = consumer.poll(timeout_ms=poll_timeout_ms)

                for _topic_partition, messages in message_pack.items():
                    for message in messages:
                        batch_buffer.append(message.value)

                # EVALUATE FLUSH CRITERIA
                # If data exists in the buffer, flush it immediately!
                # - Case A: Buffer filled up quickly due to high traffic (max_batch_size reached).
                # - Case B: Topic went silent or poll timed out, meaning we must flush the remaining data.
                if len(batch_buffer) >= max_batch_size or (batch_buffer and not message_pack):
                    chunk_df = pd.DataFrame(batch_buffer)
                    batch_buffer.clear()

                    if not chunk_df.empty:
                        yield chunk_df

                        # Transactional commit: executed only after downstream operations succeed
                        try:
                            consumer.commit()
                        except Exception as commit_fault:
                            print(f"[KAFKA COMMIT FAILURE] Offset sync failed: {commit_fault}")

        except Exception as streaming_fault:
            print(f"\n[KAFKA PIPELINE FAULT] Stream execution crashed: {streaming_fault}")
            raise streaming_fault

        finally:
            consumer.close()
