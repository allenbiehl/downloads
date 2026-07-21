import os
from typing import Any
import boto3
from botocore.config import Config

class S3ClientFactory:
    """Responsibility: Encapsulates the instantiation and configuration of S3 clients."""
    
    # Class-level member variable type declarations
    _endpoint_url: str | None
    _aws_access_key_id: str | None
    _aws_secret_access_key: str | None
    _max_workers: int

    def __init__(
            self, 
            endpoint_url: str | None = None, 
            aws_access_key_id: str | None = None, 
            aws_secret_access_key: str | None = None,
            max_workers: int = 64
    ) -> None:
        self._endpoint_url = endpoint_url or os.getenv("NETAPP_ENDPOINT")
        self._aws_access_key_id = aws_access_key_id or os.getenv("NETAPP_ACCESS_KEY")
        self._aws_secret_access_key = aws_secret_access_key or os.getenv("NETAPP_SECRET_KEY")
        self._max_workers = max_workers

    def create_client(self) -> Any:
        """Manufactures a fresh, thread-safe S3 client instance with tailored connection pooling."""
        s3_config: Config = Config(
            max_pool_connections=self._max_workers + 10,
            s3={'addressing_style': 'path'}
        )
        
        if self._endpoint_url:
            return boto3.client(
                's3',
                endpoint_url=self._endpoint_url,
                aws_access_key_id=self._aws_access_key_id,
                aws_secret_access_key=self._aws_secret_access_key,
                config=s3_config
            )
        return boto3.client('s3', config=s3_config)
