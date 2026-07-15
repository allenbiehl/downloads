# cdet_etl/infrastructure/aws_profile.py
from dataclasses import dataclass

@dataclass(frozen=True, kw_only=True)
class AwsCredentialsProfile:
    """
    Immutable, Frozen Value Object Container for AWS/MinIO Infrastructure Profiles.
    Guarantees thread-safe, unalterable connection boundaries across all dataflows.
    """
    access_key: str
    secret_key: str
    endpoint_url: str | None = None
    region: str = "us-east-1"
    ssl_verify: str = "True"
