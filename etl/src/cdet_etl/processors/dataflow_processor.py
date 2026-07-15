import pandas as pd

from cdet_etl.readers.base_data_source import BaseDataSource
from cdet_etl.readers.base_reader import BaseReader
from cdet_etl.transformers.base_transformer import BaseTransformer
from cdet_etl.writers.base_writer import BaseWriter

class DataFlowProcessor:
    """
    Agnostic Stream Execution Orchestrator.
    Consumes instantiated strategy objects to map data pipelines across flat memory bounds.
    """
    def __init__(
        self, 
        reader: BaseReader, 
        transformers: list[BaseTransformer], 
        writer: BaseWriter
    ):
        self._reader = reader
        self._transformers = transformers
        self._writer = writer

    def _apply_transformations(self, data_stream: pd.DataFrame):
        """
        Private Class Generator Strategy.
        Iterates through the incoming stream chunks and applies transformers sequentially.
        """
        for df_chunk in data_stream:
            if df_chunk.empty:
                continue

            processed_chunk = df_chunk
            for transformer in self._transformers:
                processed_chunk = transformer.transform(processed_chunk)

            if processed_chunk is not None and not processed_chunk.empty:
                yield processed_chunk

    def execute(self, *, source: BaseDataSource) -> None:
        """Type-hardened public entry point processing file batches or live event loops uniformly."""
        print(f"\n[DATAFLOW PROCESSOR] Commencing stream execution for: {source}")
        raw_data_stream = self._reader.read_stream(source=source)
        pipeline_generator = self._apply_transformations(raw_data_stream)
        self._writer.write_stream(chunk_stream=pipeline_generator, metadata=source.metadata)
