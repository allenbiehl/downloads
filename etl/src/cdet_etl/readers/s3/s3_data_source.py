import os
from dataclasses import dataclass, field
from cdet_etl.readers.base_data_source import BaseDataSource


@dataclass(frozen=True, kw_only=True)
class S3DataSource(BaseDataSource):
    """
    Immutable type container for file-system storage pathways (S3, ADLS, Local).
    
    Natively pre-compiles and formats its own file metadata boundary variables
    to protect downstream consumers from string-manipulation leaks.
    """
    uri: str
    custom_metadata: dict = field(default_factory=dict)

    @property
    def metadata(self) -> dict:
        """
        Generates a standard file tracking metadata layout on demand.
        Enforces lowercase-normalized keys to accommodate S3 metadata requirements.
        """
        base_metadata = {
            "source-uri": self.uri,
            "source-filename": os.path.basename(self.uri),
            "source-type": "s3",
            "ingestion-engine": "cdet_etl_stream_v2",
        }

        merged_metadata = base_metadata | self.custom_metadata
        return {key.lower(): str(val) for key, val in merged_metadata.items()}
