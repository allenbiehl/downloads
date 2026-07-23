# pylint: disable=missing-module-docstring
# pylint: disable=broad-exception-caught
import calendar
from datetime import datetime, timezone, timedelta
import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataset_ageoff.config.file_config_loader import FileConfigLoader
from dataset_ageoff.utils.runner import CommandRunner, ExecutionResult, ExecutionStatus
from dataset_ageoff.utils.logger import root_logger as logger

from dataset_ageoff.clients.dataset_api_client import DatasetApiClient
from dataset_ageoff.audit.models import (
    DatasetDirectory,
    AgeoffPeriodJobDetails,
    DateRangePeriod,
    AuditConfig,
    PeriodType
)

class AgeoffPeriodManager:
    """
    Generates ageoff audit for all dataset directories
    """
    _config: AuditConfig
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
        self._config = AuditConfig(
            **FileConfigLoader.load(config_path)
        )

    def execute(self, start_date: datetime, period_type: PeriodType) -> None:
        """
        Generate audit that includes files that were aged off between the start and end dates. 
        The audit is generated in parallel using multiple workers.
        """
        targets = self._dataset_api_client.get_directories()

        logger.info("Initializing parallel engine with %d concurrent workers...", self._config.max_workers)
        logger.info("Scanning %d targets over NFS. Performance tracking enabled.", len(targets))

        start_date = self._get_period_start_date(start_date=start_date, period_type=period_type)
        end_date = self._get_period_end_date(start_date=start_date, period_type=period_type)
        logger.info("Processing start date: '%s', end date: '%s'", start_date, end_date)

        jobs = self._build_jobs(
            targets=targets,
            start_date=start_date,
            end_date=end_date,
            period_type=period_type
        )

        try:
            self._execute_jobs(jobs)
        except Exception as err:
            logger.error("An error occurred, %s", err)

    def _execute_jobs(self, jobs: list[AgeoffPeriodJobDetails]) -> None:
        logger.info("Executing jobs on concurrent workers")
        with ProcessPoolExecutor(max_workers=self._config.max_workers) as executor:
            futures = {
                executor.submit(self._execute_job, job): job for job in jobs
            }
            for future in as_completed(futures):
                job = futures[future]
                result = future.result()

                if result.status == ExecutionStatus.SUCCESS:
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
                        result.duration,
                        result.duration,
                        result.error_message
                    )

    def _execute_job(self, job_details: AgeoffPeriodJobDetails) -> ExecutionResult:
        """
        Worker process that executes the shell pipeline and times the execution.
        """
        logger.info("Processing dataset '%s' directory '%s'",
            job_details.target.name, job_details.target.path)

        if not os.path.exists(job_details.target.path):
            return ExecutionResult(
                status=ExecutionStatus.SKIPPED,
                error_message=f"Path '{job_details.target.path}' not found",
                duration=0.0
            )

        cmd = self._build_command(job_details)
        return self._runner.run(cmd)

    def _build_command(self, job_details: AgeoffPeriodJobDetails) -> str:
        dir_path = job_details.target.path
        start_date = job_details.period.start_date.strftime("%Y-%m-%d %H:%M:%S")
        end_date = job_details.period.end_date.strftime("%Y-%m-%d %H:%M:%S")
        output_dir = job_details.output_dir
        current_dir = os.path.dirname(os.path.abspath(__file__))
        target_script = os.path.join(current_dir, "ageoff_writer.py")

        # mac dev env
        is_mac = sys.platform == "darwin"

        if is_mac:
            # option 2
            cmd = (
                f'find "{dir_path}" -type f -newermt "{start_date}" ! -newermt "{end_date}" -print0 | '
                f'xargs -0 stat -t "%Y-%m-%d" -f "%Sm\t%N\t%z" | '            
                f'python {target_script} --config=/{self._config_path} --output-dir="{output_dir}"'
            )
        else:
            cmd = (
                f'find "{dir_path}" -type f -newermt "{start_date}" ! -newermt "{end_date}" '
                f'-printf "%FA|||%p|||%s\\0" | '
                f'python compress_meta.py --config=/{self._config_path} --output-dir="{output_dir}"'
            )
        logger.debug(cmd)
        return cmd

    def _build_jobs(
        self,
        targets: list[DatasetDirectory],
        start_date: datetime,
        end_date: datetime,
        period_type: PeriodType
    ) -> list[AgeoffPeriodJobDetails]:
        jobs = []
        for target in targets:
            period = self._get_ageoff_period(
                target=target,
                start_date=start_date,
                end_date=end_date,
                period_type=period_type
            )

            if not period:
                logger.debug("Skipping job %s (%s). Outside ageoff period.",
                    target.name, target.path)
                continue

            output_dir = self._build_output_dir(target=target, period=period)
            job = AgeoffPeriodJobDetails(target=target, period=period, output_dir=output_dir)

            logger.debug("Adding job %s (%s).", target.name, target.path)
            jobs.append(job)

        return jobs

    def _build_output_dir(self, target: DatasetDirectory, period: DateRangePeriod) -> str:
        if period.period_type == PeriodType.DAY:
            date_prefix = period.start_date.strftime("%Y/%m/%d")
        else:
            date_prefix = period.start_date.strftime("%Y/%m")

        return os.path.join(
            "s3://",
            self._config.s3_bucket,
            self._config.s3_prefix,
            date_prefix,
            f"{target.name}"
        )

    def _get_ageoff_period(
        self,
        target: DatasetDirectory,
        start_date: datetime,
        end_date: datetime,
        period_type: PeriodType
    ) -> DateRangePeriod:
        ageoff_cutoff = datetime.now(timezone.utc) - timedelta(days=target.age_off_days)
        ageoff_cutoff = ageoff_cutoff.replace(hour=0, minute=0, second=0, microsecond=0)
        ageoff_cutoff = ageoff_cutoff - timedelta(seconds=1)

        if ageoff_cutoff <= start_date:
            return None

        if ageoff_cutoff <= end_date:
            return DateRangePeriod(
                start_date=start_date,
                end_date=ageoff_cutoff,
                period_type=period_type
            )

        return DateRangePeriod(
            start_date=start_date,
            end_date=end_date,
            period_type=period_type
        )

    def _get_period_start_date(self, start_date: datetime, period_type: PeriodType) -> datetime:
        if period_type == PeriodType.MONTH:
            return datetime(
                start_date.year, start_date.month, 1, 0, 0, 0,
                tzinfo=timezone.utc
            )
        if period_type == PeriodType.DAY:
            return datetime(
                start_date.year, start_date.month, start_date.day, 0, 0, 0,
                tzinfo=timezone.utc
            )
        raise ValueError(f"Unable to determine period end date. Invalid period type '{type}'")

    def _get_period_end_date(self, start_date: datetime, period_type: PeriodType) -> datetime:
        if period_type == PeriodType.MONTH:
            _, last_day = calendar.monthrange(start_date.year, start_date.month)
            return datetime(
                start_date.year, start_date.month, last_day, 23, 59, 59,
                tzinfo=timezone.utc
            )
        if period_type == PeriodType.DAY:
            return datetime(
                start_date.year, start_date.month, start_date.day, 23, 59, 59,
                tzinfo=timezone.utc
            )
        raise ValueError(f"Unable to determine period end date. Invalid period type '{type}'")
