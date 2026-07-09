from typing import List, Type

from memory_profiler import profile

from etl.readers.base_s3_reader import BaseS3Reader
from etl.transformers.base_transformer import BaseTransformer
from etl.writers.base_writer import BaseWriter
from etl.utils.profile import profile_etl_step

class DefaultJobManager:
    """
    Orchestration Engine.
    Dynamically executes any combination of Ingest, Transform, and Writer classes.
    """
    _readers: List[BaseS3Reader]
    _transformers: List[BaseTransformer]
    _writer: BaseWriter

    def __init__(
        self,
        readers: List[Type[BaseS3Reader]],
        transformers: List[Type[BaseTransformer]],
        writer: BaseWriter
    ):
        self._readers = [reader_cls() for reader_cls in readers]
        self._transformers = [trans_cls() for trans_cls in transformers]
        self._writer = writer()

    @profile
    @profile_etl_step("process files")
    def process_file(self, s3_path: str) -> None:
        """
        Execute
        """
        print(f"\n[ORCHESTRATOR] >>> Commencing processing loop for file: '{s3_path}'")

        selected_reader = None
        for reader_instance in self._readers:
            if reader_instance.can_handle(s3_path):
                selected_reader = reader_instance
                break

        if not selected_reader:
            print(f"[ERROR] No registered reader can handle: '{s3_path}'")
            return False

        print(f"[ORCHESTRATOR] Selected Reader Engine: '{selected_reader.__class__.__name__}'")

        try:
            current_stream = selected_reader.stream_chunks(s3_path)

            for transformer in self._transformers:
                current_stream = transformer.transform(current_stream)

            self._writer.write_stream(current_stream)

            print(f"[ORCHESTRATOR] Successfully finalized file: '{s3_path}'")
            return True

        except Exception as e:
            print(f"[CRITICAL LAYER FAULT] File execution crashed: {str(e)}")
            raise
