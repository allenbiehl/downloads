
import os
from typing import Generator

import pandas as pd

from etl.transformers.base_transformer import BaseTransformer

class StatusTransformer(BaseTransformer):
    _target_status: str

    def __init__(self):
        self._target_status = None
        self._configure_from_env()

    def _configure_from_env(self):
        self._target_status = os.getenv("TRANSFORM_STATUS_CASE", "UPPER")

    def transform(self, chunk_stream: pd.DataFrame) -> Generator[pd.DataFrame, None, None]:
        for df in chunk_stream:
            if "status" in df.columns:
                if self._target_status == "UPPER":
                    df["status"] = df["status"].str.upper()
                else:
                    df["status"] = df["status"].str.lower()
            yield df
