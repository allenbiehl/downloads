import os
import uuid
import json
from confluent_kafka import Producer
import pandas as pd

from cdet_etl.writers.base_metadata_provider import BaseMetadataProvider
from cdet_etl.writers.base_write_context import BaseWriteContext
from cdet_etl.utils.retry import execute_with_retry

class KafkaWriteContext(BaseWriteContext):
    """Hardened Kafka event writer utilizing native transaction isolation pools."""

    def __init__(self, properties: dict, metadata_provider: BaseMetadataProvider | None = None):   
        self._bootstrap_servers = properties.get("boostrap_servers")
        self._topic = properties.get("topic")
        self._message_key_field = properties.get("message_key_field")
        self._transactional_id_prefix = properties.get("transactional_id_prefix", "etl")
        self._producer = self._create_producer()

    def init_transaction(self, *, metadata: dict | None = None):
        execute_with_retry("Beging Kafka Transaction", self._producer.begin_transaction)

    def write_chunk(self, df: pd.DataFrame):
        for col in df.select_dtypes(include=['datetime64', 'datetimetz']).columns:
            df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        for record in df.to_dict(orient="records"):
            payload = json.dumps(record).encode("utf-8")
            message_key = self._get_message_key(record)
            self._producer.produce(topic=self._topic, key=message_key, value=payload)

        self._producer.poll(0)
        self._producer.flush()

    def commit_transaction(self):
        if self._producer:
            execute_with_retry("Commit Kafka Transaction", self._producer.commit_transaction)

    def abort_transaction(self):
        if self._producer:
            execute_with_retry("Abort Kafka Transaction", self._producer.abort_transaction)

    def _create_producer(self):
        conf = {
            'bootstrap.servers': self._bootstrap_servers,
            'transactional.id': self._get_deterministic_transactional_id(),
            'enable.idempotence': True,
            'acks': 'all',
            'debug': 'generic,broker,msg' 
        }
        producer = execute_with_retry("Initialize Kafka Producer", Producer, conf)
        execute_with_retry("Init Kafka Transactions", producer.init_transactions)
        return producer

    def _get_deterministic_transactional_id(self) -> str:
        """
        Generates a transactional.id that is stable across restarts 
        but unique across parallel instances in both Docker and Kubernetes.
        """
        base_name = self._transactional_id_prefix
        pod_name = os.getenv("K8S_POD_NAME")

        if pod_name:
            return f"{base_name}_{pod_name}"

        replica_index = os.getenv("DOCKER_REPLICA_INDEX")

        if replica_index:
            return f"{base_name}_replica_{replica_index}"

        hostname = os.getenv("HOSTNAME", "default_worker")
        return f"{base_name}_{hostname}"

    def _get_message_key(self, record: dict) -> str:
        if self._message_key_field:
            return str(record.get(self._message_key_field)).encode("utf-8")
        return str(uuid.uuid4()).encode("utf-8")
