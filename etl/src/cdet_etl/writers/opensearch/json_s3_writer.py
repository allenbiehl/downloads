import io
from cdet_etl.writers.s3.base_s3_write_stream import BaseS3Writer

class JsonS3Writer(BaseS3Writer):
    """Hardened S3 Multi-part writer with flat memory chunk streaming."""
    def __init__(self):
        super().__init__()
        self._s3_client = None
        self._active_uploads = {}
        self._parts_manifest = {}
        self._residual_bytes = {}
        self._MIN_PART_SIZE = 5 * 1024 * 1024

    def _init_transaction(self):
        self._s3_client = self._get_boto3_s3_client()

    def _write_chunk(self, df):
        for date_val, group in df.groupby("date_partition"):
            partition_path = f"{date_val}"
            s3_key = f"{self._output_prefix.strip('/')}/{partition_path}/part-0000.json"
            clean_group = group.drop(columns=["date_partition"])
            
            sink = io.StringIO()
            clean_group.to_json(sink, orient="records", lines=True)
            json_str = sink.getvalue()
            if json_str and not json_str.endswith("\n"):
                json_str += "\n"
                
            incoming_bytes = json_str.encode("utf-8")
            if partition_path in self._residual_bytes:
                incoming_bytes = self._residual_bytes[partition_path] + incoming_bytes
                del self._residual_bytes[partition_path]

            if partition_path not in self._active_uploads:
                init_res = self._execute_with_retry(
                    f"Create Multipart Upload for {s3_key}",
                    self._s3_client.create_multipart_upload, Bucket=self._output_bucket, Key=s3_key
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
                
                part_res = self._execute_with_retry(
                    f"Upload Part {part_num} for {s3_key}",
                    self._s3_client.upload_part,
                    Bucket=self._output_bucket, Key=s3_key, UploadId=upload_id, PartNumber=part_num, Body=chunk_to_upload
                )
                self._parts_manifest[partition_path].append({"ETag": part_res["ETag"], "PartNumber": part_num})
                start_offset = end_offset

            if len(incoming_bytes[start_offset:]) > 0:
                self._residual_bytes[partition_path] = incoming_bytes[start_offset:]

    def _commit_transaction(self):
        for partition_path, upload_id in self._active_uploads.items():
            s3_key = f"{self._output_prefix.strip('/')}/{partition_path}/part-0000.json"
            leftover = self._residual_bytes.get(partition_path, b"")
            if leftover or len(self._parts_manifest[partition_path]) == 0:
                part_num = len(self._parts_manifest[partition_path]) + 1
                part_res = self._execute_with_retry(
                    f"Final Part Upload {part_num} for {s3_key}",
                    self._s3_client.upload_part,
                    Bucket=self._output_bucket, Key=s3_key, UploadId=upload_id, PartNumber=part_num, Body=leftover
                )
                self._parts_manifest[partition_path].append({"ETag": part_res["ETag"], "PartNumber": part_num})

            self._execute_with_retry(
                f"Complete Multipart Upload for {s3_key}",
                self._s3_client.complete_multipart_upload,
                Bucket=self._output_bucket, Key=s3_key, UploadId=upload_id, MultipartUpload={"Parts": self._parts_manifest[partition_path]}
            )

    def _abort_transaction(self):
        for partition_path, upload_id in self._active_uploads.items():
            s3_key = f"{self._output_prefix.strip('/')}/{partition_path}/part-0000.json"
            self._execute_with_retry(
                f"Abort Multipart Upload for {s3_key}",
                self._s3_client.abort_multipart_upload, Bucket=self._output_bucket, Key=s3_key, UploadId=upload_id
            )
