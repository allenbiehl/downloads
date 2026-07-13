import pyarrow.fs as pafs
from cdet_etl.models.aws_credentials_profile import AwsCredentialsProfile

class PyArrowS3FileSystemFactory:
    """Native C++ Storage Layer Allocation Factory driven by frozen parameter injection."""

    @classmethod
    def create(cls, profile: AwsCredentialsProfile) -> pafs.S3FileSystem:
        """Builds a native C++ S3FileSystem pointer for the injected credentials profile."""
        return pafs.S3FileSystem(
            access_key=profile.access_key,
            secret_key=profile.secret_key,
            region=profile.region,
            endpoint_override=cls.__get_endpoint(profile),
            scheme=cls.__get_scheme(profile),
            force_virtual_addressing=False
        )

    @staticmethod
    def __get_scheme(profile: AwsCredentialsProfile) -> str:
        if profile.endpoint_url and profile.endpoint_url.startswith("http://"):
            return "http"
        return "https"

    @staticmethod
    def __get_endpoint(profile: AwsCredentialsProfile) -> str:
        endpoint = profile.endpoint_url
        if endpoint and "://" in endpoint:
            endpoint = endpoint.split("://")[-1]
        return endpoint
