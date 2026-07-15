import os

def get_env(key: str, prefix: str, default: str = None):
    """
    Get environment variable with optional prefix
    """
    if prefix:
        prefix = f"{prefix.upper()}_"
    return os.getenv(f"{prefix}{key}", default)

