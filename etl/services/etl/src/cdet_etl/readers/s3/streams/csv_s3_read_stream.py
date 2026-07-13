import os
import pyarrow as pa
import pyarrow.dataset as ds

from cdet_etl.readers.s3.s3_data_source import S3DataSource
from cdet_etl.readers.s3.streams.base_s3_read_stream import BaseS3ReadStream

class CsvS3ReadStream(BaseS3ReadStream):
    """Declarative streaming chunk consumer for JSON and JSON Lines format."""
    def can_handle(self, source: S3DataSource) -> bool:
        _, ext = os.path.splitext(source.uri.lower())
        return ext in (".csv")

    def stream_chunks(self, uri: str):
        """Standardized S3 Data Engineering Stream Provider."""
        clean_s3_path = self._get_clean_s3_path(uri)
        dataset = ds.dataset(clean_s3_path, format=ds.CsvFileFormat(), filesystem=self._s3_fs)
        for record_batch in dataset.to_batches():
            yield pa.Table.from_batches([record_batch]).to_pandas()

