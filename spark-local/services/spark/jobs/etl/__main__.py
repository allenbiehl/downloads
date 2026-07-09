import os

from etl.job_managers.default_job_manager import DefaultJobManager
from etl.readers.parquet_s3_reader import ParquetS3Reader
from etl.readers.json_s3_reader import JsonS3Reader
from etl.transformers.status_transformer import StatusTransformer
from etl.transformers.partition_date_transformer import PartitionDateTransformer
from etl.writers.json_s3_writer import JsonS3Writer

if __name__ == "__main__":
    # Simulate the environment configurations supplied to your host system
    os.environ["INGEST_S3_PATH"] = "s3://input/mock_geo_data_100mb.json" # Auto-detects JSON
    os.environ["OUTPUT_S3_BUCKET"] = "output"
    os.environ["OUTPUT_S3_PREFIX"] = "nospark/"
    os.environ["AWS_ACCESS_KEY_ID"] = "admin"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "password"
    os.environ["AWS_ENDPOINT_URL"] = "http://minio:9000" # MinIO / LocalStack compatible
    os.environ["TRANSFORM_STATUS_CASE"] = "UPPER"

    # 2. NEW Sink Configurations (Plugging in Kafka environment settings)
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "localhost:9092"
    os.environ["KAFKA_TOPIC_PREFIX"] = "telemetry.geo.date."
    os.environ["KAFKA_TRANSACTIONAL_ID"] = "prod_geo_stream_pipeline"

    os.environ["OPENSEARCH_HOST"] = "localhost"
    os.environ["OPENSEARCH_PORT"] = "9200"
    os.environ["OPENSEARCH_INDEX"] = "company_historical_logs"

    readers = [ParquetS3Reader, JsonS3Reader]
    transformers = [PartitionDateTransformer, StatusTransformer]

    pipeline_job = DefaultJobManager(
        readers=readers,
        transformers=transformers,
        writer=JsonS3Writer
    )

    batch_files = [
        "s3://input/mock_geo_data_50mb.json"
    ]

    for file_path in batch_files:
        pipeline_job.process_file(file_path)
