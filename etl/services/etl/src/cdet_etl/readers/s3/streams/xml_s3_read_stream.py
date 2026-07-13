import os
import pandas as pd
import xml.etree.ElementTree as ET

from cdet_etl.readers.s3.s3_data_source import S3DataSource
from cdet_etl.readers.s3.streams.base_s3_read_stream import BaseS3ReadStream
from cdet_etl.utils.xml_schema_config import XmlSchemaConfigLoader

class XmlS3ReadStream(BaseS3ReadStream):
    """Custom XML stream parser driven by external schema and type configurations."""
    def __init__(self):
        super().__init__()
        self._row_tag = os.getenv("INGEST_XML_ROW_TAG", "record")
        self._chunk_size = int(os.getenv("INGEST_CHUNK_SIZE", "10000"))
        self._field_names = None
        self._type_mappings = {} # Format: {"col_name": "float" | "int" | "string"}
        env_config_target = os.getenv("INGEST_XML_SCHEMA_CONFIG")
        self._field_names, self._type_mappings = XmlSchemaConfigLoader.load_configuration(env_config_target)

    def can_handle(self, source: S3DataSource) -> bool:
        _, ext = os.path.splitext(source.uri.lower())
        return ext in (".xml")

    def stream_chunks(self, source: S3DataSource):
        clean_path = self._get_clean_s3_path(source.uri)
        
        # 1. AUTO-DETECT FALLBACK: If no explicit configuration was provided, discover columns
        if not self._field_names:
            self._field_names = self._discover_schema_from_stream(clean_path)
            
        native_stream = self._s3_fs.open_input_file(clean_path)
        columns_data = {name: [] for name in self._field_names}
        buffer_count = 0

        with native_stream as stream_bytes:
            context = ET.iterparse(stream_bytes, events=("end",))
            for _event, elem in context:
                if elem.tag == self._row_tag:
                    buffer_count += 1
                    
                    for name in self._field_names:
                        child = elem.find(name)
                        val = child.text if child is not None else None
                        
                        # 2. DYNAMIC EXTERNAL CASTING ENGINE
                        if val is not None:
                            target_type = self._type_mappings.get(name)
                            if target_type == "float":
                                val = float(val)
                            elif target_type == "int":
                                val = int(val)
                            # Strings require no manual casting; they default natively
                                
                        columns_data[name].append(val)

                    elem.clear()
                    if buffer_count >= self._chunk_size:
                        yield pd.DataFrame(columns_data)
                        columns_data = {name: [] for name in self._field_names}
                        buffer_count = 0
            
            if buffer_count > 0:
                yield pd.DataFrame(columns_data)

    def _discover_schema_from_stream(self, clean_path: str, sample_records: int = 5) -> list:
        """Pre-flight discovery: Reads a tiny slice of the file to auto-detect tag schemas."""
        print(f"[SCHEMA DETECT] Auto-detecting XML columns from '{clean_path}' using tag '<{self._row_tag}>'...")
        discovered_fields = set()
        records_scanned = 0
        
        native_stream = self._s3_fs.open_input_file(clean_path)
        with native_stream as stream_bytes:
            context = ET.iterparse(stream_bytes, events=("end",))
            for _event, elem in context:
                if elem.tag == self._row_tag:
                    records_scanned += 1
                    for child in elem:
                        if child.tag:
                            discovered_fields.add(child.tag)
                    
                    elem.clear()
                    if records_scanned >= sample_records:
                        break
                        
        if not discovered_fields:
            raise ValueError(f"Schema discovery failed. Could not find row tag '<{self._row_tag}>' inside XML file.")
            
        detected_list = sorted(list(discovered_fields))
        print(f"[SCHEMA DETECT] Successfully discovered {len(detected_list)} columns: {detected_list}")
        return detected_list