from datetime import datetime
import uuid
from cdet_etl.writers.base_metadata_provider import BaseMetadataProvider
from cdet_etl.writers.base_write_context import BaseWriteContext
from cdet_etl.writers.base_write_stream import BaseWriteStream
from cdet_etl.writers.s3.metadata.default_metadata_provider import DefaultMetadataProvider
from cdet_etl.writers.s3.streams import JsonS3WriteStream, ParquetS3WriteStream
from cdet_etl.utils.s3_client_factory import S3ClientFactory
from cdet_etl.models.aws_credentials_profile import AwsCredentialsProfile
from cdet_etl.utils.retry import execute_with_retry

class S3WriteContext(BaseWriteContext):
    """Internal S3 Transport Strategy Engine. Encapsulates active multipart upload states."""
    
    _STREAM_REGISTRY = {
        "json": JsonS3WriteStream,
        "parquet": ParquetS3WriteStream
    }

    def __init__(self, properties: dict, metadata_provider: BaseMetadataProvider | None = None):
        self._format_type = properties.get("format", "json").lower()
        self._bucket_name = properties.get("bucket_name")
        self._object_prefix = properties.get("object_prefix", "")
        self._partition = properties.get("partition", "date_partition")

        self._metadata_provider = metadata_provider or DefaultMetadataProvider()
        self._stream_engine = self._create_stream_engine(self._format_type)
        
        profile = AwsCredentialsProfile(
            access_key = properties.get("access_key"),
            secret_key = properties.get("secret_key"),
            endpoint_url = properties.get("endpoint_url"),
            region = properties.get("region", "us-east-1"),
            ssl_verify = properties.get("aws_ssl_verify", "True")
        )
        self._s3_client = S3ClientFactory.create(profile)

        self._job_token = ""
        self._metadata = None
        self._active_uploads = {}
        self._parts_manifest = {}
        self._residual_bytes = {}
        self._MIN_PART_SIZE = 5 * 1024 * 1024

    def _create_stream_engine(self, format_type: str) -> BaseWriteStream:
        stream_cls = self._STREAM_REGISTRY.get(format_type)
        if not stream_cls:
            raise ValueError(f"Unsupported write format capability target: '{format_type}'")
        return stream_cls()

    def _build_s3_key(self, partition_path: str) -> str:
        return f"{self._object_prefix}/{partition_path}/part-{self._job_token}.{self._format_type}"

    def init_transaction(self, *, metadata: dict | None = None):
        """Resets transactional tracking maps while anchoring the active source file lineage."""
        self._active_uploads.clear()
        self._parts_manifest.clear()
        self._residual_bytes.clear()
        self._metadata = metadata

        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        self._job_token = f"{timestamp}-{uuid.uuid4().hex[:6]}"

        print("[S3 SINK] Opened new file transaction loop block.")
        print(f"[S3 SINK] Target Format: '{self._format_type}'")
        print(f"[S3 SINK] Generated Dynamic Object Token: {self._job_token}")        

    def write_chunk(self, df):
        for partition_val, group in df.groupby(self._partition):
            partition_path = partition_val
            s3_key = self._build_s3_key(partition_path)
            clean_group = group.drop(columns=[self._partition])
            incoming_bytes = self._stream_engine.format_chunk(partition_path, clean_group)

            if partition_path in self._residual_bytes:
                incoming_bytes = self._residual_bytes[partition_path] + incoming_bytes
                del self._residual_bytes[partition_path]

            if len(incoming_bytes) == 0:
                continue

            if partition_path not in self._active_uploads:
                upload_kwargs = self._metadata_provider.get_upload_kwargs(metadata=self._metadata)
                upload_kwargs["Bucket"] = self._bucket_name
                upload_kwargs["Key"] = s3_key

                init_res = execute_with_retry(
                    action_name=f"Init Multipart Upload for {s3_key}",
                    func=self._s3_client.create_multipart_upload, 
                    **upload_kwargs
                )
                self._active_uploads[partition_path] = init_res["UploadId"]
                self._parts_manifest[partition_path] = []

            upload_id = self._active_uploads[partition_path]
            total_len = len(incoming_bytes)
            start_offset = 0

            while (total_len - start_offset) >= self._MIN_PART_SIZE:
                end_offset = start_offset + self._MIN_PART_SIZE
                chunk_to_upload = incoming_bytes[start_offset:end_offset]

                part_num = len(self._parts_manifest[partition_path]) + 1
                part_res = execute_with_retry(
                    action_name=f"Upload Part {part_num} for {s3_key}",
                    func=self._s3_client.upload_part,
                    Bucket=self._bucket_name,
                    Key=s3_key,
                    UploadId=upload_id,
                    PartNumber=part_num,
                    Body=chunk_to_upload
                )
                self._parts_manifest[partition_path].append({"ETag": part_res["ETag"], "PartNumber": part_num})
                start_offset = end_offset

            if len(incoming_bytes[start_offset:]) > 0:
                self._residual_bytes[partition_path] = incoming_bytes[start_offset:]

    def commit_transaction(self):
        print("\n--- COMMITTING ALL ACTIVE CONFIGURATION SINK TARGETS ---")
        for partition_path, upload_id in list(self._active_uploads.items()):
            s3_key = self._build_s3_key(partition_path)
            final_metadata_bytes = self._stream_engine.close_and_finalize(partition_path)
            leftover = self._residual_bytes.get(partition_path, b"") + final_metadata_bytes

            if leftover or len(self._parts_manifest[partition_path]) == 0:
                part_num = len(self._parts_manifest[partition_path]) + 1
                part_res = execute_with_retry(
                    action_name=f"Final Dynamic Part Upload {part_num} for {s3_key}",
                    func=self._s3_client.upload_part,
                    Bucket=self._bucket_name,
                    Key=s3_key,
                    UploadId=upload_id,
                    PartNumber=part_num,
                    Body=leftover
                )
                self._parts_manifest[partition_path].append({"ETag": part_res["ETag"], "PartNumber": part_num})

            execute_with_retry(
                action_name=f"Complete Multipart Upload for {s3_key}",
                func=self._s3_client.complete_multipart_upload,
                Bucket=self._bucket_name,
                Key=s3_key,
                UploadId=upload_id,
                MultipartUpload={"Parts": self._parts_manifest[partition_path]}
            )

        print(f"Success! Transaction committed safely for token: {self._job_token}")

    def abort_transaction(self):
        for partition_path in list(self._active_uploads.keys()):
            try:
                self._stream_engine.close_and_finalize(partition_path)
            except Exception:
                pass
                
        for partition_path, upload_id in self._active_uploads.items():
            s3_key = self._build_s3_key(partition_path)
            execute_with_retry(
                action_name=f"Abort Multipart Upload for {s3_key}",
                func=self._s3_client.abort_multipart_upload,
                Bucket=self._bucket_name,
                Key=s3_key,
                UploadId=upload_id
            )
