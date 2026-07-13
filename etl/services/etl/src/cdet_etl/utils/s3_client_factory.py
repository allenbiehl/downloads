import boto3
from botocore.client import Config
from cdet_etl.models.aws_credentials_profile import AwsCredentialsProfile

class S3ClientFactory:

    @classmethod
    def create(cls, profile: AwsCredentialsProfile):
        """Builds a ready-to-use boto3 S3 Client for the injected credentials profile."""
        return boto3.client(
            "s3",
            aws_access_key_id=profile.access_key,
            aws_secret_access_key=profile.secret_key,
            region_name=profile.region,
            endpoint_url=profile.endpoint_url or None,
            use_ssl=cls.__use_ssl(profile),
            verify=cls.__verify_ssl(profile),
            config=Config(signature_version='s3v4', s3={'addressing_style': 'path'})
        )

    @staticmethod
    def __verify_ssl(profile: AwsCredentialsProfile) -> bool:
        if not profile.ssl_verify or profile.ssl_verify.lower() in ("false", "0", "off"):
            return False
        return True

    @staticmethod
    def __use_ssl(profile: AwsCredentialsProfile) -> bool:
        if not profile.endpoint_url:
            return False
        return profile.endpoint_url.startswith("https://")
