from abc import ABC, abstractmethod

from cdet_etl.models.aws_credentials_profile import AwsCredentialsProfile
from cdet_etl.readers.s3.s3_data_source import S3DataSource
from cdet_etl.utils.pyarrow_s3_fs_factory import PyArrowS3FileSystemFactory

class BaseS3ReadStream(ABC):
    """
    Template Base Class for S3/MinIO Ingestors.
    Exclusively utilizes PyArrow's C++ S3FileSystem for unified network pooling.
    """
    def __init__(self, properties: dict = None):
        self._properties = properties
        profile = AwsCredentialsProfile(
            access_key = properties.get("access_key"),
            secret_key = properties.get("secret_key"),
            endpoint_url = properties.get("endpoint_url"),
            region = properties.get("region", "us-east-1"),
            ssl_verify = properties.get("ssl_verify", "True")
        )
        self._s3_fs = PyArrowS3FileSystemFactory.create(profile)

    @abstractmethod
    def can_handle(self, source: S3DataSource) -> bool:
        """Enforces extension and format boundary checks across the file link nodes."""

    @abstractmethod
    def stream_chunks(self, source: S3DataSource):
        """Yields sequential Pandas DataFrames by running zero-seek streaming loops."""

    def _get_clean_s3_path(self, source: S3DataSource) -> str:
        clean = source.uri
        for proto in ["s3://", "http://", "https://"]:
            if clean.startswith(proto):
                clean = clean[len(proto):]
        return clean
