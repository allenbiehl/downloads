import pyarrow.dataset as ds

from etl.readers.base_s3_reader import BaseS3Reader

class CsvS3Reader(BaseS3Reader):
    """High-speed CSV data ingestor."""
    @property
    def _file_format(self) -> ds.FileFormat:
        return ds.CsvFileFormat()

    def can_handle(self, s3_path: str) -> bool:
        return s3_path.lower().endswith(".csv")
