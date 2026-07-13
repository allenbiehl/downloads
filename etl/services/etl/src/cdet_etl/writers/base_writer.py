from cdet_etl.writers.base_write_context import BaseWriteContext

class BaseWriter:
    """Agnostic public entry shell enforcing a hook-free contract."""
    def __init__(self, context: BaseWriteContext):
        self._context = context

    def write_stream(self, chunk_stream, *, metadata: dict | None = None):
        """The ONLY public method on the writer interface."""
        try:
            self._context.init_transaction(metadata=metadata)
            for df in chunk_stream:
                if df.empty:
                    continue
                self._context.write_chunk(df)
            self._context.commit_transaction()
        except Exception as pipeline_fault:
            print(f"\n[CRITICAL SINK FAULT] Transaction failed: {pipeline_fault}. Rolling back...")
            try:
                self._context.abort_transaction()
            except Exception as rollback_failure:
                print(f"[FATAL UNWOUND ENGINE] Stranded data risk! Abort failed: {rollback_failure}")
            raise pipeline_fault
