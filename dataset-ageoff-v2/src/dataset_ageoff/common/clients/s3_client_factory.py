import os
from dataclasses import dataclass, field
from typing import Any
import boto3
from botocore.config import Config

@dataclass(frozen=True)
class S3ClientFactoryConfig:
    """Configuration blueprint for the S3/NetApp Creational Factory."""
    endpoint_url: str | None = field(default_factory=lambda: os.getenv("S3_ENDPOINT"))
    aws_access_key_id: str | None = field(default_factory=lambda: os.getenv("S3_ACCESS_KEY"))
    aws_secret_access_key: str | None = field(default_factory=lambda: os.getenv("S3_SECRET_KEY"))
    max_workers: int = field(default=16)

class S3ClientFactory:
    """Responsibility: Encapsulates infrastructure properties and builds a thread-safe S3 client."""
    
    _config: S3ClientFactoryConfig

    def __init__(self, config: S3ClientFactoryConfig | None = None) -> None:
        self._config = config or S3ClientFactoryConfig()

    def create_client(self) -> Any:
        """Manufactures a fresh, thread-safe S3 client instance with optimized connection pooling."""
        s3_config: Config = Config(
            max_pool_connections=self._config.max_workers + 10,
            s3={'addressing_style': 'path'}
        )
        
        if self._config.endpoint_url:
            return boto3.client(
                's3',
                endpoint_url=self._config.endpoint_url,
                aws_access_key_id=self._config.aws_access_key_id,
                aws_secret_access_key=self._config.aws_secret_access_key,
                config=s3_config
            )
        return boto3.client('s3', config=s3_config)
