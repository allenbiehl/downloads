import pandas as pd

from cdet_etl.transformers.base_transformer import BaseTransformer

class DatePartitionTransformer(BaseTransformer):
    """
    KISS-Compliant Structural Date Partitioning Filter.
    Extracts chronological event indices from individual materialized DataFrame chunks.
    """
    def __init__(self, properties: dict | None = None):
        super().__init__(properties)
        self._timestamp_field = self._properties.get("timestamp_field", "event_time")

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Processes a single isolated dataframe slice synchronously."""
        if df.empty or "event_time" not in df.columns:
            return df

        # Natively extract the year/month/day folder hierarchy mapping
        df["date_partition"] = pd.to_datetime(df[self._timestamp_field], utc=True).dt.strftime("%Y/%m/%d")
        
        return df