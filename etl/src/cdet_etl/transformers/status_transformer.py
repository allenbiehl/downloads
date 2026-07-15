import pandas as pd

from cdet_etl.transformers.base_transformer import BaseTransformer

class StatusTransformer(BaseTransformer):

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Processes a single isolated dataframe slice synchronously with zero copy cost."""
        if df.empty or "status" not in df.columns:
            return df    

        # df["status"] = df["status"].str.upper()

        return df