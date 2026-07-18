# pylint: disable=missing-module-docstring
import io
import sys
import argparse
from dataset_ageoff.config.file_config_loader import FileConfigLoader
from dataset_ageoff.inventory.models import InventoryConfig
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as pafs

from dataset_ageoff.utils.logger import root_logger as logger

class InventoryWriter:
    _config: InventoryConfig
    _output_dir: str
    _batch_size: int
    _fs: pafs.S3FileSystem
    _active_streams: list
    _active_writers: list

    def __init__(self, config_path: str, output_dir: str):
        logger.info("Initializing %s", self.__class__.__name__)
        self._config = InventoryConfig(
            **FileConfigLoader.load(config_path)
        )
        self._output_dir = output_dir.rstrip('/')
        self._schema = pa.schema([
            ('path', pa.string()),
            ('size_bytes', pa.int64()),
            ('modified_date', pa.string())
        ])
        self._fs = self._init_file_system()
        self._active_streams = []
        self._active_writers = []

    def write(self, stream: io.BufferedReader) -> None:
        """
        Write stream to s3 output dir
        """
        self._empty_output_dir()
        self._write_stream(stream)

    def _init_file_system(self):
        logger.info("Initializing S3 file system")
        return pafs.S3FileSystem(
            access_key=self._config.s3_access_key,
            secret_key=self._config.s3_secret_key,
            endpoint_override=self._config.s3_endpoint_override,
            scheme=self._config.s3_scheme,
            force_virtual_addressing=self._config.s3_force_virtual_addressing,
            region=self._config.s3_region
        )

    def _empty_output_dir(self) -> None:
        clean_dir_path = self._get_clean_s3_path(self._output_dir)
        try:
            self._fs.delete_dir_contents(clean_dir_path)
            logger.info("Successfully cleaned output dir '%s'", self._output_dir)
        except Exception as err:
            logger.error("Failed to clean output dir '%s', %s", self._output_dir, err)

    def _write_stream(self, stream: io.BufferedReader) -> None:
        logger.info("Writing output stream to '%s'", self._output_dir)
        paths, sizes, dates = [], [], []
        total_processed, part_index = 0, 0
        buffer = b""

        try:
            while chunk := stream.read(65536):
                buffer += chunk 

                while b'\x00' in buffer:
                    line_bytes, buffer = buffer.split(b'\x00', 1)
                    line = line_bytes.decode('utf-8', errors='replace').strip()
                    if not line:
                        continue

                    parts = line.split('|||')
                    if len(parts) != 3:
                        continue

                    dates.append(parts[0][:10])
                    paths.append(parts[1])
                    sizes.append(int(parts[2]))
                    total_processed += 1

                    if len(paths) >= self._config.batch_size:
                        self._stage_file_part(part_index, paths, sizes, dates)
                        part_index += 1
                        paths, sizes, dates = [], [], []

            if paths:
                self._stage_file_part(part_index, paths, sizes, dates)
                part_index += 1
                paths, sizes, dates = [], [], []

            self._commit(part_index, total_processed)


        except Exception as err:
            self._rollback(err)
            raise err

    def _stage_file_part(self, part_index: int, paths: list, sizes: list, dates: list) -> None:
        """Instantiates the multipart stream handle and stages the file part without committing."""
        file_path = f"{self._output_dir}/part_{part_index}.parquet"
        logger.info("Staging file part '%s' via multipart upload", file_path)
        clean_file_path = self._get_clean_s3_path(file_path)
        s3_file = self._fs.open_output_stream(clean_file_path)
        writer = pq.ParquetWriter(s3_file, self._schema, compression='zstd', compression_level=3)
        batch = pa.RecordBatch.from_arrays([paths, sizes, dates], schema=self._schema)
        writer.write_batch(batch)
        self._active_streams.append(s3_file)
        self._active_writers.append(writer)

    def _commit(self, part_count: int, total_processed: int) -> None:
        """Appends Parquet footers and executes CompleteMultipartUpload on all files."""
        logger.info("Stream fully read with zero errors. Committing all files...")
        for writer in self._active_writers:
            writer.close()
        for s3_file in self._active_streams:
            s3_file.close()
        logger.info("Transaction committed! Generated %d Parquet part files. Total records: %d",
            part_count, total_processed)

    def _rollback(self, error: Exception) -> None:
        """Aborts all active multipart streams, cleaning up uncommitted chunks on MinIO."""
        logger.error("Aborting transaction. Purging all uncommitted multipart chunks..., %s", error)
        for writer in self._active_writers:
            try:
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
    parser.add_argument(
        "--config", 
        help="Config file"
    )
    parser.add_argument(
        "--output-dir", 
        help="Target storage folder path (e.g. bucket-name/folder)",
    )
    args = parser.parse_args()
    InventoryWriter(config_path=args.config, output_dir=args.output_dir).write(sys.stdin.buffer)
