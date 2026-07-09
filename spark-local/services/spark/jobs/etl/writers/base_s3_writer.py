import os
import boto3
from botocore.client import Config
from etl.writers.base_writer import BaseWriter

class BaseS3Writer(BaseWriter):
    """
    Protected Intermediate Class for S3/MinIO Sinks.
    Consolidates S3 infrastructure variables, protocol stripping, and client pooling.
    """
    def __init__(self):
        super().__init__()
        self._access_key = os.getenv("AWS_ACCESS_KEY_ID")
        self._secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self._endpoint_url = os.getenv("AWS_ENDPOINT_URL")
        self._output_bucket = os.getenv("OUTPUT_S3_BUCKET")
        self._output_prefix = os.getenv("OUTPUT_S3_PREFIX")
        self._ssl_verify = self._parse_ssl_verify(os.getenv("AWS_SSL_VERIFY", "True"))

    def _parse_ssl_verify(self, env_value: str):
        if not env_value: return True
        if env_value.lower() in ("false", "0", "off"): return False
        if env_value.lower() in ("true", "1", "on"): return True
        return env_value

    def _get_boto3_s3_client(self):
        """Protected factory constructing standard boto3 configurations."""
        is_http = self._endpoint_url.startswith("http://") if self._endpoint_url else False
        return boto3.client(
            "s3",
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
            endpoint_url=self._endpoint_url,
            use_ssl=False if is_http else True,
            verify=self._ssl_verify,
            config=Config(signature_version='s3v4', s3={'addressing_style': 'path'})
        )
