import json
from abc import ABC, abstractmethod
import os
import yaml


class BaseConfigLoader(ABC):
    """Abstract template base for format-specific configuration links."""

    @abstractmethod
    def can_handle(self, file_path: str) -> bool:
        """Enforces format compliance validation across the chain links."""

    @abstractmethod
    def load(self, file_path: str) -> dict:
        """Parses a target configuration file layout into a standard python dictionary."""

    def _load_content(self, file_path: str) -> str:
        """Protected utility isolating raw filesystem read operations."""
        try:
            with open(file_path, encoding="utf-8") as file_stream:
                return file_stream.read()
        except FileNotFoundError as err:
            raise FileNotFoundError(
                f"Target configuration blueprint asset was not found at path: '{file_path}'"
            ) from err


class YamlConfigLoader(BaseConfigLoader):
    """Handles native YAML structure configuration loading."""

    def can_handle(self, file_path: str) -> bool:
        return file_path.lower().endswith((".yaml", ".yml"))

    def load(self, file_path: str) -> dict:
        try:
            return yaml.safe_load(self._load_content(file_path))
        except yaml.YAMLError as err:
            raise ValueError(
                f"[YAML PARSE STRUCTURE ERROR] Indentation or schema syntax violation: {err}"
            ) from err


class JsonConfigLoader(BaseConfigLoader):
    """Handles native JSON structure configuration loading."""

    def can_handle(self, file_path: str) -> bool:
        return file_path.lower().endswith((".json",))

    def load(self, file_path: str) -> dict:
        try:
            return json.loads(self._load_content(file_path))
        except json.JSONDecodeError as err:
            raise ValueError(
                f"[JSON PARSE SYNTAX ERROR] Malformed token found at line {err.lineno}, "
                f"column {err.colno}: {err.msg}"
            ) from err


class FileConfigLoader:
    """
    Stateless public entry orchestrator.
    Coordinates configuration parsing via a clean Chain of Responsibility.
    """

    _LOADERS = [
        YamlConfigLoader(),
        JsonConfigLoader()
    ]

    @classmethod
    def load_from_file(cls, file_path: str) -> dict:
        """
        Reads an external absolute file path and decodes it into a standard python dictionary.
        """
        if not file_path:
            raise ValueError("A valid file path target string must be provided.")

        for loader in cls._LOADERS:
            if loader.can_handle(file_path):
                return loader.load(file_path)

        raise TypeError(
            f"Unsupported configuration extension format for file: '{file_path}'. "
            f"No registered loader link was able to handle the file layout signature."
        )
