import pyarrow.dataset as ds

from etl.readers.base_s3_reader import BaseS3Reader

class ParquetS3Reader(BaseS3Reader):
    """High-speed Parquet data ingestor."""
    @property
    def _file_format(self) -> ds.FileFormat:
        return ds.ParquetFileFormat()

    def can_handle(self, s3_path: str) -> bool:
        return s3_path.lower().endswith(".parquet")