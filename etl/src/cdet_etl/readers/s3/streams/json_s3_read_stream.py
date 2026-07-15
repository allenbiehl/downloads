import math
import os
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.json as pajson

from cdet_etl.readers.s3.s3_data_source import S3DataSource
from cdet_etl.readers.s3.streams.base_s3_read_stream import BaseS3ReadStream

class JsonS3ReadStream(BaseS3ReadStream):
    """
    High-Performance Unified JSON Streaming Consumer.
    
    Processes multi-line arrays, pretty-printed adjacent object streams, 
    and flat JSONL at native C++ compilation speeds using a single network pass.
    """
    def can_handle(self, source: S3DataSource) -> bool:
        _, ext = os.path.splitext(source.uri.lower())
        return ext in (".json", ".jsonl")

    def stream_chunks(self, source: S3DataSource):
        """Streams JSON payloads chunk-by-chunk using a single continuous network stream."""
        clean_s3_path = self._get_clean_s3_path(source)
        network_chunk_size = 5 * 1024 * 1024  # Standard 5MB chunk window

        # 1. Execute exactly ONE single continuous network read operation
        with self._s3_fs.open_input_file(clean_s3_path) as file_stream:
            raw_bytes = file_stream.read()
            
        if not raw_bytes:
            return

        # Check for the traditional JSON array opening bracket boundary
        is_json_array = b"[" in raw_bytes[:100].strip()

        # --- STREAMING PATH A: Standard JSON Array Tree ([...]) ---
        if is_json_array:
            buffer = pa.BufferReader(raw_bytes)
            # Native Pandas memory byte array mapping processes standard array contexts flawlessly
            full_df = pd.read_json(buffer, orient="records")

            if not full_df.empty:
                # In-memory chunk slicing to fulfill the downstream stream contract requirements
                total_rows = len(full_df)
                chunk_rows = 10000
                num_chunks = math.ceil(total_rows / chunk_rows)

                for i in range(num_chunks):
                    start_idx = i * chunk_rows
                    end_idx = start_idx + chunk_rows
                    chunk_df = full_df.iloc[start_idx:end_idx]
                    if not chunk_df.empty:
                        yield chunk_df

        # --- STREAMING PATH B: Multi-Line Adjacent Objects (Format 2) & Flat JSON Lines (Format 3) ---
        else:
            # Wrap the raw un-seeked byte stream directly inside an in-memory C++ buffer reader
            buffer = pa.BufferReader(raw_bytes)

            # High-Performance C++ Parsing Directives:
            # Setting newlines_in_values=True tells the native stream parser to track matching braces
            # across multi-line breaks, processing format 2 and format 3 flawlessly.
            read_options = pajson.ReadOptions(block_size=network_chunk_size)
            parse_options = pajson.ParseOptions(newlines_in_values=True)
            
            # Fire the high-performance C++ streaming reader straight-through the memory block
            reader = pajson.open_json(buffer, read_options=read_options, parse_options=parse_options)

            # Convert the stream record batches to standard data frames sequentially
            for record_batch in reader:
                chunk_df = record_batch.to_pandas()
                if not chunk_df.empty:
                    yield chunk_df



# class JsonS3ReadStream(BaseS3ReadStream):
#     """
#     High-Performance Unified JSON Streaming Consumer.
    
#     Processes multi-line arrays, pretty-printed adjacent object streams, 
#     and flat JSONL at native C++ compilation speeds using a single network pass.
#     """
#     def __init__(self, properties: dict = None):
#         super().__init__(properties)
#         # Standard 10MB chunk boundary to accommodate large staging files
#         self._network_chunk_size = 10 * 1024 * 1024
#         self._chunk_rows = 10000

#     def can_handle(self, uri: str) -> bool:
#         _, ext = os.path.splitext(uri.lower())
#         return ext in (".json", ".jsonl")

#     def stream_chunks(self, uri: str):
#         """Streams JSON payloads chunk-by-chunk using a single continuous network stream."""
#         clean_s3_path = self._get_clean_s3_path(uri)

#         with self._s3_fs.open_input_file(clean_s3_path) as file_stream:
#             raw_bytes = file_stream.read()
            
#         if not raw_bytes:
#             return

#         if self._is_json_array(raw_bytes):
#             return self._stream_json_format(raw_bytes)
#         else:
#             return self._stream_jsonl_format(raw_bytes)
        
#     def _is_json_array(self, raw_bytes: any) -> bool:
#         return b"[" in raw_bytes[:100].strip()
    
#     def _stream_json_format(self, raw_bytes):
#         buffer = pa.BufferReader(raw_bytes)
#         full_df = pd.read_json(buffer, orient="records")

#         if not full_df.empty:
#             total_rows = len(full_df)
#             num_chunks = math.ceil(total_rows / self._chunk_rows)

#             for i in range(num_chunks):
#                 start_idx = i * self._chunk_rows
#                 end_idx = start_idx + self._chunk_rows
#                 chunk_df = full_df.iloc[start_idx:end_idx]
#                 if not chunk_df.empty:
#                     yield chunk_df

#     def _stream_jsonl_format(self, raw_bytes):
#         # Wrap the raw un-seeked byte stream directly inside an in-memory C++ buffer
#         buffer = pa.BufferReader(raw_bytes)

#         # High-Performance C++ Parsing Directives:
#         # Enforcing block sizes matching our file payload prevents allocation breaks.
#         # Setting newlines_in_values=False lets the engine scan record objects lightning-fast.
#         read_options = pajson.ReadOptions(block_size=self._network_chunk_size)
#         parse_options = pajson.ParseOptions(newlines_in_values=False)
        
#         # Fire the high-performance C++ parser straight-through the memory block
#         pyarrow_table = pajson.read_json(buffer, read_options=read_options, parse_options=parse_options)

#         # Convert the table to standard record batches to preserve chunk streaming
#         for record_batch in pyarrow_table.to_batches(max_chunksize=10000):
#             chunk_df = record_batch.to_pandas()
#             if not chunk_df.empty:
#                 yield chunk_df

#     @property
#     def _file_format(self) -> ds.FileFormat:
#         return ds.JsonFileFormat()
