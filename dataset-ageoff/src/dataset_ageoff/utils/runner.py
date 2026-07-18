import subprocess
import time

from dataset_ageoff.inventory.models import JobExecutionResult, JobExecutionStatus

class CommandRunner:
    """A reusable executor for running shell commands with real-time streaming and timeouts."""
    
    def __init__(self, default_timeout: float = 60.0):
        self.default_timeout: float = default_timeout
        self._current_process: subprocess.Popen | None = None

    def run(self, command: str | list[str], timeout_seconds: float | None = None) -> JobExecutionResult:
        """Executes a command, streams stdout/stderr, and returns a structured CommandResult with duration."""
        timeout: float = timeout_seconds if timeout_seconds is not None else self.default_timeout
        use_shell: bool = isinstance(command, str)
        start_time: float = time.perf_counter()
        
        self._current_process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            shell=use_shell
        )

        try:
            while self._current_process.poll() is None:
                if time.perf_counter() - start_time > timeout:
                    raise TimeoutError(f"Command timed out after {timeout} seconds.")

                line: str = self._current_process.stdout.readline()
                if line:
                    print(line, end="")
                else:
                    time.sleep(0.05)

            for line in self._current_process.stdout:
                print(line, end="")

            duration = time.perf_counter() - start_time
            exit_code = self._current_process.returncode
            status = JobExecutionStatus.SUCCESS if exit_code == 0 else JobExecutionStatus.FAILURE
            return JobExecutionResult(status=status, duration=duration, exit_code=exit_code)

        except TimeoutError as err:
            duration = time.perf_counter() - start_time
            return JobExecutionResult(status=JobExecutionStatus.FAILURE, duration=duration, error_message=str(err))
        except Exception as err:
            duration = time.perf_counter() - start_time
            return JobExecutionResult(status=JobExecutionStatus.FAILURE, duration=duration, error_message=f"Unexpected error: {err}")
        finally:
            self._cleanup_process()
            self._current_process = None

    def _cleanup_process(self) -> None:
        """Safely terminates the active process if running."""
        if self._current_process and self._current_process.poll() is None:
            self._current_process.terminate()
            try:
                self._current_process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self._current_process.kill()
