# pylint: disable=missing-module-docstring
# pylint: disable=broad-exception-caught
from datetime import datetime, timezone, timedelta
import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataset_ageoff.config.file_config_loader import FileConfigLoader
from dataset_ageoff.utils.runner import CommandRunner
from dataset_ageoff.utils.logger import root_logger as logger

from dataset_ageoff.clients.dataset_api_client import DatasetApiClient
from dataset_ageoff.inventory.models import (
    DatasetDirectory,
    AgeoffPendingJobDetails,
    JobExecutionResult,
    JobExecutionStatus,
    InventoryConfig
)

class AgeoffPendingManager:
    """
    Generates ageoff inventory for all dataset directories
    """
    _config: InventoryConfig
    _dataset_api_client: DatasetApiClient
    _runner: CommandRunner

    def __init__(
        self,
        config_path: str,
        dataset_api_client: DatasetApiClient | None = None,
        runner: CommandRunner | None = None
    ):
        self._config_path = config_path
        self._dataset_api_client = dataset_api_client or DatasetApiClient()
        self._runner = runner or CommandRunner()
        self._config = InventoryConfig(
            **FileConfigLoader.load(config_path)
        )

    def execute(self) -> None:
        """
        Generate inventory that includes files that were aged off between the start and end dates. 
        The inventory is generated in parallel using multiple workers.
        """
        targets = self._dataset_api_client.get_directories()

        print(f"Initializing parallel engine with {self._config.max_workers} concurrent workers...")
        print(f"Scanning {len(targets)} targets over NFS. Performance tracking enabled.")

        jobs = self._build_jobs(targets=targets)

        try:
            self._execute_jobs(jobs)
        except Exception as err:
            print(f"An error occurred, {err}")

    def _execute_jobs(self, jobs: list[AgeoffPendingJobDetails]) -> None:
        print("Executing jobs on concurrent workers")
        with ProcessPoolExecutor(max_workers=self._config.max_workers) as executor:
            futures = {
                executor.submit(self._execute_job, job): job for job in jobs
            }
            for future in as_completed(futures):
                job = futures[future]
                result = future.result()

                if result.status == JobExecutionStatus.SUCCESS:
                    logger.info("[Finished] %s (%s) in %s -> %s",
                        job.target.name,
                        job.target.path,
                        result.duration,
                        job.output_dir
                    )
                else:
                    logger.warning("[Alert] %s (%s) in %s -> %s: %s",
                        job.target.name,
                        job.target.path,
                        result.duraction,
                        result.duration,
                        result.error_message
                    )

    def _execute_job(self, job_details: AgeoffPendingJobDetails) -> JobExecutionResult:
        """
        Worker process that executes the shell pipeline and times the execution.
        """
        logger.info("Processing dataset '%s' directory '%s'", 
            job_details.target.name, job_details.target.path)

        if not os.path.exists(job_details.target.path):
            return JobExecutionResult(
                status=JobExecutionStatus.SKIPPED,
                error_message=f"Path '{job_details.target.path}' not found",
                duration=0.0
            )

        cmd = self._build_command(job_details)
        return self._runner.run(cmd)

    def _build_command(self, job_details: AgeoffPendingJobDetails) -> str:
        dir_path = job_details.target.path
        ageoff_date = job_details.ageoff_date.strftime("%Y-%m-%d %H:%M:%S")
        output_dir = job_details.output_dir

        # Dynamically locate the target script relative to this file's directory
        current_dir = os.path.dirname(os.path.abspath(__file__))
        target_script = os.path.join(current_dir, "inventory_writer.py")

        # mac dev env
        is_mac = sys.platform == "darwin"

        if is_mac:
            cmd = (
                f'find "{dir_path}" -type f ! -newermt "{ageoff_date}" -print0 | '
                f'xargs -0 stat -t "%Y-%m-%d" -f "%Sm|||%N|||%z" | '
                f'tr "\\n" "\\0" | '
                f'sort -z | '
                f'python {target_script} --config=/{self._config_path} --output-dir="{output_dir}"'
            )
        else:
            cmd = (
                f'find "{dir_path}" -type f ! -newermt "{ageoff_date}" '
                f'-printf "%FA|||%p|||%s\\0" | '
                f'sort -z | '
                f'python compress_meta.py --config=/{self._config_path} --output-dir="{output_dir}"'
            )
        return cmd

    def _build_jobs(self, targets: list[DatasetDirectory]) -> list[AgeoffPendingJobDetails]:
        return [AgeoffPendingJobDetails(
                target=target,
                ageoff_date=self._get_ageoff_date(target),
                output_dir=self._build_output_dir(target)
            )
            for target in targets
        ]

    def _build_output_dir(self, target: DatasetDirectory) -> str:
        return os.path.join("s3://", self._config.s3_bucket, self._config.s3_prefix, f"{target.name}")

    def _get_ageoff_date(self, target: DatasetDirectory) -> datetime:
        ageoff_cutoff = datetime.now(timezone.utc) - timedelta(days=target.age_off_days)
        ageoff_cutoff = ageoff_cutoff.replace(hour=0, minute=0, second=0, microsecond=0)
        ageoff_cutoff = ageoff_cutoff - timedelta(seconds=1)

        return ageoff_cutoff
