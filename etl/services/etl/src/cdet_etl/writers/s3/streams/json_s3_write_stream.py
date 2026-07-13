import io
import pandas as pd
from cdet_etl.writers.base_write_stream import BaseWriteStream

class JsonS3WriteStream(BaseWriteStream):
    """State-free internal serializing engine for JSON Lines target streams."""
    def format_chunk(self, partition_path: str, group_df: pd.DataFrame) -> bytes:
        sink = io.StringIO()
        group_df.to_json(sink, orient="records", lines=True)
        json_str = sink.getvalue()
        if json_str and not json_str.endswith("\n"):
            json_str += "\n"
        return json_str.encode("utf-8")

    def close_and_finalize(self, partition_path: str) -> bytes:
        return b""  # JSON Lines requires no trailing metadata flags
