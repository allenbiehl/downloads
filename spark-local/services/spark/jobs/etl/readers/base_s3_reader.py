import os
from abc import abstractmethod
from pyarrow import fs
import pyarrow as pa
import pyarrow.dataset as ds
from etl.readers.base_reader import BaseReader

class BaseS3Reader(BaseReader):
    """
    Template Base Class for S3/MinIO Ingestors.
    Exclusively utilizes PyArrow's C++ S3FileSystem for unified network pooling.
    """
    def __init__(self):
        self._access_key = os.getenv("AWS_ACCESS_KEY_ID")
        self._secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self._endpoint_url = os.getenv("AWS_ENDPOINT_URL")
        self._region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        self._ssl_verify = self._parse_ssl_verify(os.getenv("AWS_SSL_VERIFY", "True"))
        self._s3_fs = self._initialize_s3_filesystem()

    def _parse_ssl_verify(self, env_value: str):
        """Evaluates whether to return a boolean toggle or a CA bundle filepath string."""
        if env_value.lower() in ("false", "0", "off"):
            return False
        if env_value.lower() in ("true", "1", "on"):
            return True
        return env_value

    def _initialize_s3_filesystem(self) -> fs.S3FileSystem:
        """Boots PyArrow's C++ S3 file management layer once at startup."""
        return fs.S3FileSystem(
            access_key=self._access_key,
            secret_key=self._secret_key,
            region=self._region,
            endpoint_override=self._get_endpoint_override(),
            scheme=self._get_scheme(),
            force_virtual_addressing=False 
        )

    def _get_endpoint_override(self) -> str:
        if not self._endpoint_url:
            return ""
        return self._endpoint_url.replace("http://", "").replace("https://", "")

    def _get_scheme(self) -> str:
        if not self._endpoint_url or self._endpoint_url.startswith("https://"):
            return "https"
        return "http"

    def _get_clean_s3_path(self, raw_s3_path: str) -> str:
        """Protected utility to turn any S3 URI protocol into a clean 'bucket/key' string."""
        clean = raw_s3_path
        for proto in ["s3://", "http://", "https://"]:
            if clean.startswith(proto):
                clean = clean[len(proto):]
        return clean

    def stream_chunks(self, s3_path: str):
        """
        Standardized Data Engineering Stream Provider.
        Reused by all native formats; overridden exclusively by complex types like XML.
        """
        clean_s3_path = self._get_clean_s3_path(s3_path)
        dataset = ds.dataset(clean_s3_path, format=self._file_format, filesystem=self._s3_fs)
        for record_batch in dataset.to_batches():
            yield pa.Table.from_batches([record_batch]).to_pandas()

    @property
    @abstractmethod
    def _file_format(self) -> ds.FileFormat:
        """Enforces child classes to return their native PyArrow FileFormat instance."""
