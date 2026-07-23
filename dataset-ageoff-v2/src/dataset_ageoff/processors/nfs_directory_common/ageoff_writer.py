# pylint: disable=missing-module-docstring
import io
import json
import sys
import uuid
import argparse
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import pyarrow.fs as pafs
import pyarrow.compute as pc

from dataset_ageoff.common.config.file_config_loader import FileConfigLoader
from dataset_ageoff.processors.nfs_directory_common.models import AuditConfig
from dataset_ageoff.common.utils.logger import root_logger as logger

class AgeoffWriter:
    _config: AuditConfig
    _output_dir: str
    _schema: pa.Schema
    _fs: pafs.S3FileSystem
    _active_streams: list
    _active_writers: list
    _csv_read_options: pacsv.ReadOptions
    _csv_parse_options: pacsv.ParseOptions
    _csv_convert_options: pacsv.ConvertOptions

    def __init__(self, config_path: str, output_dir: str):
        self._config = AuditConfig(**FileConfigLoader.load(config_path))
        self._output_dir = output_dir.rstrip('/')
        self._schema = self._create_schema()
        self._fs = self._create_file_system()
        self._csv_parse_options = self._create_csv_parse_options()
        self._csv_read_options = self._create_csv_read_options()
        self._csv_convert_options = self._create_csv_convert_options()
        self._active_streams = []
        self._active_writers = []

    def write(self, stream: io.BufferedReader) -> None:
        """
        Write stream to s3 output dir
        """
        logger.info("Writing output stream to '%s' using native pre-tabbed C++ parser...", self._output_dir)
        try:
            self._empty_output_dir()
            global_table = self._ingest_stream(stream)
            
            if not global_table:
                logger.info("No records to write to S3.")
                return

            global_table = self._sort_files(global_table)
            self._write_file_parts(global_table)
            self._write_global_json_summary(global_table)
            self._commit(global_table.num_rows)
        except Exception as err:
            self._rollback(err)
            raise err

    def _create_file_system(self) -> pafs.S3FileSystem:
        return pafs.S3FileSystem(
            access_key=self._config.s3_access_key,
            secret_key=self._config.s3_secret_key,
            endpoint_override=self._config.s3_endpoint_override,
            scheme=self._config.s3_scheme,
            force_virtual_addressing=self._config.s3_force_virtual_addressing,
            region=self._config.s3_region
        )

    def _create_schema(self) -> pa.Schema:
        return pa.schema([
            ('path', pa.string()),
            ('size_bytes', pa.int64()),
            ('modified_date', pa.string())
        ])

    def _create_csv_parse_options(self) -> pacsv.ParseOptions:
        return pacsv.ParseOptions(
            delimiter="\t",
            quote_char=False,
            invalid_row_handler=lambda row: "skip"
        )

    def _create_csv_convert_options(self) -> pacsv.ConvertOptions:
        return pacsv.ConvertOptions(
            column_types={
                "f0": pa.string(),
                "f1": pa.string(),
                "f2": pa.int64()
            },
            include_columns=["f0", "f1", "f2"]
        )

    def _create_csv_read_options(self) -> pacsv.ReadOptions:
        return pacsv.ReadOptions(
            autogenerate_column_names=True,
            block_size=10 * 1024 * 1024  # Continuous 10MB chunk pipeline buffer
        )

    def _empty_output_dir(self) -> None:
        clean_dir_path = self._get_clean_s3_path(self._output_dir)
        try:
            self._fs.delete_dir_contents(clean_dir_path)
            logger.info("Successfully cleaned output dir '%s'", self._output_dir)
        except Exception as err:
            if "Path does not exist" in str(err):
                return
            logger.error("Failed to clean output dir '%s', %s", self._output_dir, err)

    def _write_global_json_summary(self, global_table: pa.Table) -> None:
        """Computes total dataset metrics and flushes them to a JSON file on MinIO."""
        logger.info("Generating global dataset JSON summary file...")
        
        # Calculate totals natively in C++ memory across the table (Static lookup, O(1) property)
        total_records = global_table.num_rows
        combined_size = pc.sum(global_table.column("size_bytes")).as_py()

        summary_data = {
            "total_parts": len(self._active_streams),          
            "total_bytes": str(combined_size),
            "total_records": total_records
        }

        summary_file_path = f"{self._output_dir}/summary.json"
        clean_file_path = self._get_clean_s3_path(summary_file_path)
        
        # Open an output stream handle directly onto MinIO
        s3_file = self._fs.open_output_stream(clean_file_path)
        
        json_bytes = json.dumps(summary_data).encode('utf-8')
        s3_file.write(json_bytes)
        
        # Register the stream handle into the transactional lifecycle lists
        self._active_streams.append(s3_file)
        self._active_writers.append(s3_file)

    def _ingest_stream(self, stream) -> pa.Table | None:
        global_table = pacsv.read_csv(
            stream,
            read_options=self._csv_read_options,
            parse_options=self._csv_parse_options,
            convert_options=self._csv_convert_options
        )
        
        if global_table.num_rows == 0:
            return None

        # Reconstruct columns directly to match the pre-defined target schema layout.
        return pa.Table.from_arrays(
            [global_table.column("f1"), global_table.column("f2"), global_table.column("f0")],
            schema=self._schema
        )

    def _sort_files(self, global_table) -> any:
        logger.info("Performing global hierarchical sort on %d records by date, then path...", global_table.num_rows)
        return global_table.sort_by([
            ("modified_date", "ascending"),
            ("path", "ascending")
        ])

    def _write_file_parts(self, global_table) -> None:
        total_processed = global_table.num_rows
        part_index = 0
        
        for offset in range(0, total_processed, self._config.batch_size):
            slice_chunk = global_table.slice(offset, self._config.batch_size)
            self._stage_file_part_table(part_index, slice_chunk)
            part_index += 1

    def _stage_file_part_table(self, part_index: int, table_chunk: pa.Table) -> None:
        """Instantiates a multipart stream handle and writes a sorted PyArrow Table slice natively."""
        file_id = uuid.uuid4().hex
        file_path = f"{self._output_dir}/part_{file_id}.parquet"
        logger.info("Staging file part '%s' via multipart upload", file_path)
        
        clean_file_path = self._get_clean_s3_path(file_path)
        s3_file = self._fs.open_output_stream(clean_file_path)
        
        writer = pq.ParquetWriter(s3_file, self._schema, compression='zstd', compression_level=3)
        writer.write_table(table_chunk)
        
        self._active_streams.append(s3_file)
        self._active_writers.append(writer)

    def _commit(self, total_processed: int) -> None:
        """Appends Parquet footers and executes CompleteMultipartUpload on all files."""
        logger.info("Committing transaction. Completing all uncommitted multipart chunks for '%s'",
            self._output_dir)
        total_parts = len(self._active_streams)
        for writer in self._active_writers:
            if hasattr(writer, 'close') and writer is not self._active_streams:
                writer.close()
        for s3_file in self._active_streams:
            s3_file.close()
        logger.info("Transaction committed! Generated %d data part files for %s. Total records: %d",
            total_parts, self._output_dir, total_processed)

    def _rollback(self, error: Exception) -> None:
        """Aborts all active multipart streams, cleaning up uncommitted chunks on MinIO."""
        logger.info("Aborting transaction. Purging all uncommitted multipart chunks for '%s', %s",
            self._output_dir, error)
        for writer in self._active_writers:
            try:
                if hasattr(writer, 'close') and writer is not self._active_streams:
                    writer.close()
            except Exception:
                pass
        for s3_file in self._active_streams:
            try:
                s3_file.abort()
            except Exception:
                pass

    def _get_clean_s3_path(self, uri: str) -> str:
        clean = uri
        for proto in ["s3://", "http://", "https://"]:
            if clean.startswith(proto):
                clean = clean[len(proto):]
        return clean


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Stream find outputs over a network transaction safely into compressed Parquet parts."
    )
    parser.add_argument("--config", help="Config file")
    parser.add_argument("--output-dir", help="Target storage folder path (e.g. bucket-name/folder)")
    args = parser.parse_args()
    AgeoffWriter(config_path=args.config, output_dir=args.output_dir).write(sys.stdin.buffer)
