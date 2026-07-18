import os
import re

class EnvPropertyResolver:
    """
    Stateless Infrastructure Utility.
    Recursively scans and interpolates environment property tokens to decouple runtime configurations.
    """
    _TOKEN_PATTERN = re.compile(r"\$\{(\w+)\}")

    @staticmethod
    def resolve_properties(data: dict | list | str) -> dict:
        """
        Recursively steps through configuration trees to substitute environment variable tokens.
        Returns a completely fresh, resolved data tree to preserve immutability.
        """
        if isinstance(data, dict):
            return {key: EnvPropertyResolver.resolve_properties(val) for key, val in data.items()}
        
        if isinstance(data, list):
            return [EnvPropertyResolver.resolve_properties(item) for item in data]

        if isinstance(data, str):
            match = EnvPropertyResolver._TOKEN_PATTERN.fullmatch(data)
            if match:
                env_var_name = match.group(1)
                return os.environ.get(env_var_name, "")
            
            def replace_token(m):
                return os.environ.get(m.group(1), "")
            return EnvPropertyResolver._TOKEN_PATTERN.sub(replace_token, data)

        return data
