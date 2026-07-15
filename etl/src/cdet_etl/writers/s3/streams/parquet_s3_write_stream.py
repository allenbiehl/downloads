import io
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from cdet_etl.writers.base_write_stream import BaseWriteStream

class ParquetS3WriteStream(BaseWriteStream):
    """Stateful intermediate formatting engine for Parquet columnar streams."""
    def __init__(self):
        self._compiled_buffers = {}
        self._parquet_writers = {}

    def format_chunk(self, partition_path: str, group_df: pd.DataFrame) -> bytes:
        arrow_table = pa.Table.from_pandas(group_df)

        if partition_path not in self._parquet_writers:
            memory_sink = io.BytesIO()
            pq_engine = pq.ParquetWriter(memory_sink, arrow_table.schema, compression="snappy")
            self._compiled_buffers[partition_path] = memory_sink
            self._parquet_writers[partition_path] = pq_engine

        self._parquet_writers[partition_path].write_table(arrow_table)

        memory_sink = self._compiled_buffers[partition_path]
        bytes_out = memory_sink.getvalue()
        
        memory_sink.seek(0)
        memory_sink.truncate(0)
        return bytes_out

    def close_and_finalize(self, partition_path: str) -> bytes:
        if partition_path not in self._parquet_writers:
            return b""
            
        self._parquet_writers[partition_path].close()
        
        memory_sink = self._compiled_buffers[partition_path]
        final_footer_bytes = memory_sink.getvalue()
        
        del self._parquet_writers[partition_path]
        del self._compiled_buffers[partition_path]
        return final_footer_bytes