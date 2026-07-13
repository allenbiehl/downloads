import os
import json
from confluent_kafka import Producer, KafkaException
import pandas as pd

from cdet_etl.writers.base_writer import BaseWriter

class KafkaWriter(BaseWriter):
    """Hardened Kafka event writer utilizing native transaction isolation pools."""
    def __init__(self):
        super().__init__()
        self._bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self._topic_prefix = os.getenv("KAFKA_TOPIC_PREFIX", "analytics.events.")
        self._transactional_id = os.getenv("KAFKA_TRANSACTIONAL_ID", "etl_geo_job_stream")
        self._producer = None

    def _init_transaction(self):
        conf = {
            'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            'transactional.id': self._transactional_id,
            'enable.idempotence': True,
            'acks': 'all'
        }
        # Initialize and register the transaction with the Kafka cluster coordinator (3x Retries)
        self._producer = self._execute_with_retry("Initialize Kafka Producer", Producer, conf)
        self._execute_with_retry("Init Kafka Transactions", self._producer.init_transactions, timeout=5.0)
        self._producer.begin_transaction()

    def _write_chunk(self, df: pd.DataFrame):
        for date_val, group in df.groupby("date_partition"):
            target_topic = f"{self._topic_prefix}{date_val.replace('-', '.')}"
            clean_group = group.drop(columns=["date_partition"])
            
            for record in clean_group.to_dict(orient="records"):
                payload = json.dumps(record).encode("utf-8")
                message_key = str(record.get("event_id", "")).encode("utf-8")
                
                # Asynchronously write to Kafka's cluster log
                self._producer.produce(topic=target_topic, key=message_key, value=payload)
                
        # Flush the network ring buffer after each chunk iteration to keep RAM completely flat
        self._producer.flush(timeout=0)

    def _commit_transaction(self):
        self._execute_with_retry("Commit Kafka Transaction", self._producer.commit_transaction, timeout=10.0)

    def _abort_transaction(self):
        if self._producer:
            self._execute_with_retry("Abort Kafka Transaction", self._producer.abort_transaction, timeout=5.0)
