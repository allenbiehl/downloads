from dataclasses import dataclass
import enum
import subprocess
import sys
import time

class ExecutionStatus(enum.Enum):
    SUCCESS = enum.auto()
    FAILURE = enum.auto()
    SKIPPED = enum.auto()

@dataclass
class ExecutionResult:
    status: ExecutionStatus
    duration: float = 0.0
    exit_code: int | None = None
    error_message: str | None = None

class CommandRunner:
    """A reusable executor that runs shell commands with streaming output and robust cleanup hooks."""

    def __init__(self):
        self._current_process: subprocess.Popen | None = None

    def _cleanup_process(self) -> None:
        """Terminates or kills the active subprocess if it is still running."""
        if self._current_process and self._current_process.poll() is None:
            try:
                self._current_process.terminate()
                self._current_process.wait(timeout=1.0)
            except subprocess.TimeoutExpired:
                self._current_process.kill()
                self._current_process.wait()

    def run(self, command: str, timeout: float | None = None) -> ExecutionResult:
        """Executes a command string, streams output instantly, and safely traps exceptions."""
        start_time = time.perf_counter()
        
        try:
            self._current_process = subprocess.Popen(
                command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=False
            )

            # Stream chunks immediately as they hit the OS buffer
            while True:
                # Enforce a non-blocking timeout check if one is supplied
                if timeout and (time.perf_counter() - start_time) > timeout:
                    raise TimeoutError(f"Command timed out after {timeout} seconds")

                output_chunk = self._current_process.stdout.read1()
                if not output_chunk and self._current_process.poll() is not None:
                    break
                    
                if output_chunk:
                    sys.stdout.buffer.write(output_chunk)
                    sys.stdout.flush()

            # Catch any final bytes remaining after process termination
            final_bytes = self._current_process.stdout.read()
            if final_bytes:
                sys.stdout.buffer.write(final_bytes)
                sys.stdout.flush()

            duration = time.perf_counter() - start_time
            exit_code = self._current_process.returncode
            status = ExecutionStatus.SUCCESS if exit_code == 0 else ExecutionStatus.FAILURE
            return ExecutionResult(status=status, duration=duration, exit_code=exit_code)

        except TimeoutError as err:
            duration = round(time.perf_counter() - start_time, 4)
            return ExecutionResult(status=ExecutionStatus.FAILURE, duration=duration, error_message=str(err))
        except Exception as err:
            duration = round(time.perf_counter() - start_time, 4)
            return ExecutionResult(status=ExecutionStatus.FAILURE, duration=duration, error_message=f"Unexpected error: {err}")
        finally:
            self._cleanup_process()
            self._current_process = None
