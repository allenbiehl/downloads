from typing import Generator

import pandas as pd

from etl.transformers.base_transformer import BaseTransformer

class PartitionDateTransformer(BaseTransformer):
    
    def transform(self, chunk_stream: pd.DataFrame) -> Generator[pd.DataFrame, None, None]:
        for df in chunk_stream:
            df["date_partition"] = pd.to_datetime(df["event_time"], utc=True).dt.strftime("%Y/%m/%d")
            yield df
