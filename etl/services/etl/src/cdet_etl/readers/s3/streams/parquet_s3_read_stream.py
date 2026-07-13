import os
import pyarrow as pa
import pyarrow.dataset as ds

from cdet_etl.readers.s3.s3_data_source import S3DataSource
from cdet_etl.readers.s3.streams.base_s3_read_stream import BaseS3ReadStream

class ParquetS3ReadStream(BaseS3ReadStream):
    """Declarative streaming chunk consumer for Parquet format."""
    def can_handle(self, source: S3DataSource) -> bool:
        _, ext = os.path.splitext(source.uri.lower())
        return ext in (".parquet", ".pq")
    
    def stream_chunks(self, uri: str):
        """Standardized S3 Data Engineering Stream Provider."""
        clean_s3_path = self._get_clean_s3_path(uri)
        dataset = ds.dataset(clean_s3_path, format=ds.ParquestFileFormat(), filesystem=self._s3_fs)
        for record_batch in dataset.to_batches():
            yield pa.Table.from_batches([record_batch]).to_pandas()

    @property
    def _file_format(self) -> ds.FileFormat:
        return ds.ParquetFileFormat()
