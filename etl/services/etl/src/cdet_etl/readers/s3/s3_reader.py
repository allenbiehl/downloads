from cdet_etl.readers.base_data_source import BaseDataSource
from cdet_etl.readers.base_reader import BaseReader
from cdet_etl.readers.s3.s3_data_source import S3DataSource
from cdet_etl.readers.s3.streams import CsvS3ReadStream, JsonS3ReadStream, ParquetS3ReadStream, XmlS3ReadStream
from cdet_etl.readers.s3.streams.json_s3_read_stream import JsonS3ReadStream
from cdet_etl.readers.s3.streams.parquet_s3_read_stream import ParquetS3ReadStream


class S3Reader(BaseReader):
    """
    Public Parameterized S3 Chain Reading Engine.
    Exposes exactly one public method with zero internal structural leakage.
    """
    _STRATEGY_REGISTRY = {
        "csv": CsvS3ReadStream,
        "json": JsonS3ReadStream,
        "parquet": ParquetS3ReadStream,
        "xml": XmlS3ReadStream
    }

    def __init__(self, properties: dict):
        super().__init__(properties)
        self._chain = self._build_format_chain()

    def _build_format_chain(self):
        """Assembles the internal Chain of Responsibility links."""
        formats = [fmt.lower() for fmt in self._properties.get("formats", ["json"])]
        chain = []
        for fmt in formats:
            if fmt not in self._STRATEGY_REGISTRY:
                supported = ", ".join(self._STRATEGY_REGISTRY.keys())
                raise ValueError(
                    f"[CONFIG ERROR] Unsupported format requested: '{fmt}'. "
                    f"Supported formats for S3Reader: [{supported}]"
                )
            
            strategy_cls = self._STRATEGY_REGISTRY.get(fmt)
            chain.append(strategy_cls(properties=self._properties))
        return chain

    def _evaluate_chain(self, source: S3DataSource):
        """Evaluates file paths across the internal strategy chain links array."""
        handler_found = False
        for strategy in self._chain:
            if strategy.can_handle(source):
                handler_found = True
                yield from strategy.stream_chunks(source)
                break
                
        if not handler_found:
            raise TypeError(f"Unsupported file extension layout for: '{source.uri}'")

    def read_stream(self, source: BaseDataSource):
        if not isinstance(source, S3DataSource):
            raise TypeError(f"S3Reader requires a S3DataSource instance, received: '{type(source).__name__}'")
            
        try:
            yield from self._evaluate_chain(source)
            
        except Exception as read_fault:
            print(f"\n[S3 READER FAULT] Ingestion failed for file {source.uri}: {read_fault}")
            raise read_fault
