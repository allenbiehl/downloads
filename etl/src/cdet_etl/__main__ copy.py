# cdet_etl/__main__.py
import os
import json
import pika
from cdet_etl.core.sources import FileSource
from cdet_etl.infrastructure.file_repository import MultiFileConfigRepository
from cdet_etl.infrastructure.redis_repository import RedisConfigRepository
from cdet_etl.orchestration.registry import DataFlowRegistry


def process_queue_message(channel, method, properties, body, *, registry, allowed_flows):
    """
    Transactional RabbitMQ Event Callback.
    Coordinates incoming task payloads and drives the dataflow engine.
    """
    try:
        # 1. Parse the incoming RabbitMQ message envelope payload
        # Expected structure: {"dataflow_id": "dataflow-1", "s3_bucket": "source-stage", "s3_key": "raw_logs.json"}
        payload = json.loads(body.decode("utf-8"))
        dataflow_id = payload.get("dataflow_id")
        bucket = payload.get("s3_bucket")
        key = payload.get("s3_key")

        # 2. Dynamic Routing Guard: Verify this node is authorized to handle the pipeline
        if dataflow_id not in allowed_flows:
            print(f"[ROUTING BYPASS] Dataflow '{dataflow_id}' is not allowed on this pod. Requeuing...")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            return

        print(f"\n[RABBITMQ EVENT] Processing task for pipeline: '{dataflow_id}'")
        print(f"[LINEAGE SOURCE] Targeting file pointer: s3://{bucket}/{key}")

        # 3. CONSTRUCT THE TYPE-SAFE FILE SOURCE CONTRACT
        # Maps the dynamic S3 file pointer natively into your reader hierarchy
        source_token = FileSource(uri=f"s3://{bucket}/{key}")

        # 4. FETCH EXECUTABLE PIPELINE PAIR FROM CACHE
        # Misses automatically fetch properties from Redis/Files and resolve env tokens
        processor, _ = registry.get_pipeline(dataflow_id=dataflow_id)

        # 5. RUN STRATEGY: Read source S3 -> Transform -> Write target alternate S3
        # Chunks flow sequentially under a locked, low flat memory allocation ceiling
        processor.execute(source=source_token)

        # 6. TRANSACTION SUCCESS ACKNOWLEDGEMENT
        # Only advance the RabbitMQ queue offset once the alternate S3 write completes safely!
        print(f"[SUCCESS] Ingestion and target transit finalized for: {dataflow_id}")
        channel.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as execution_fault:
        print(f"[TRANSACTION FAILURE] Pipeline crashed: {execution_fault}. Rolling back...")
        # Reject and requeue the message so another pod replica in the cluster can retry processing
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def main():
    print("=" * 60)
    print("[COMPUTE CONTAINER] Starting Low-Code Micro-Batch Kernel Worker...")
    print("=" * 60)

    # Resolve cluster allocation metadata parameters from the environment
    raw_allowed_flows = os.getenv("ALLOWED_DATAFLOWS", "")
    config_backend_type = os.getenv("CONFIG_BACKEND", "FILE").upper()
    config_dir_path = os.getenv("CONFIG_REPOSITORY_PATH", "/etc/cdet-etl/configs")
    rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672//")
    queue_name = os.getenv("RABBITMQ_QUEUE", "cdet_etl_jobs")

    if not raw_allowed_flows:
        print("[CRITICAL] ALLOWED_DATAFLOWS list is missing! Exiting...")
        sys.exit(1)

    allowed_dataflows = {flow.strip() for flow in raw_allowed_flows.split(",") if flow.strip()}

    # Initialize configuration backend storage strategy
    if config_backend_type == "REDIS":
        # config_backend = RedisConfigRepository(...)
        pass
    else:
        config_backend = MultiFileConfigRepository(directory_path=config_dir_path)

    pipeline_registry = DataFlowRegistry(repository_backend=config_backend)

    # Connect to the RabbitMQ Broker Queue Cluster
    params = pika.URLParameters(rabbitmq_url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    # Enforce standard competing-consumers pre-fetch limit
    # This prevents one pod from hoarding messages, supporting clean horizontal autoscaling!
    channel.basic_qos(prefetch_count=1)
    channel.queue_declare(queue=queue_name, durable=True)

    print(f"[NODE ONLINE] Listening to RabbitMQ Queue '{queue_name}'. Awaiting work payloads...")

    # Establish the open-ended event listening consumer callback block
    on_message_callback = lambda ch, method, properties, body: process_queue_message(
        ch, method, properties, body, registry=pipeline_registry, allowed_flows=allowed_dataflows
    )
    
    channel.basic_consume(queue=queue_name, on_message_callback=on_message_callback)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("[NODE OFFLINE] Shutting down cleanly.")
        channel.stop_consuming()
        connection.close()


if __name__ == "__main__":
    main()
