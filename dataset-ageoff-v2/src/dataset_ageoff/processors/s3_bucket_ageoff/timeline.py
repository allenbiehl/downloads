from dataclasses import dataclass
from datetime import datetime, timedelta
from dataset_ageoff.common.models.dataset_source import DatasetSource
from dataset_ageoff.processors.s3_bucket_ageoff.models import ExtractionTask
from dateutil.relativedelta import relativedelta
from typing import List
import uuid

@dataclass(frozen=True)
class LifecycleTimelineCalculatorConfig:
    """Configuration blueprint for the temporal age-off timeline scheduler."""
    age_off_days: int

class LifecycleTimelineCalculator:
    """Responsibility: Dynamically generates optimal whole-month blocks for history and precise daily blocks."""
    
    _config: LifecycleTimelineCalculatorConfig
    _cutoff_date: datetime
    _current_year_month_str: str

    def __init__(self, config: LifecycleTimelineCalculatorConfig) -> None:
        self._config = config
        now: datetime = datetime.now()
        self._cutoff_date = now - timedelta(days=self._config.age_off_days)
        self._current_year_month_str = now.strftime("%Y/%m")

    @property
    def cutoff_date(self) -> datetime:
        return self._cutoff_date

    def generate_tasks(self, start_year: int, source_config: DatasetSource) -> List[ExtractionTask]:
        tasks: List[ExtractionTask] = []
        current_date: datetime = datetime(year=start_year, month=1, day=1)
        
        if current_date > self._cutoff_date:
            return tasks

        while current_date <= self._cutoff_date:
            eval_year_month_str: str = current_date.strftime("%Y/%m")
            tx_id: str = str(uuid.uuid4())[:8]

            # CASE A: Historical Month Horizon
            if eval_year_month_str != self._current_year_month_str:
                month_prefix: str = current_date.strftime("%Y/%m/")
                # FIX: Inject source_name into path layout -> .../project_A/bucket_1/
                output_path: str = f"ageoff/period/{current_date.strftime('%Y/%m')}/{source_config.dataset_name}/{source_config.source_name}"
                
                tasks.append(
                    ExtractionTask(source_config=source_config, prefix=month_prefix, output_s3_path=output_path, transaction_id=tx_id)
                )
                current_date += relativedelta(months=1)

            # CASE B: Active Calendar Month Horizon
            else:
                day_prefix: str = current_date.strftime("%Y/%m/%d/")
                # FIX: Inject source_name into path layout -> .../project_A/bucket_2/
                output_path: str = f"ageoff/period/{current_date.strftime('%Y/%m/%d')}/{source_config.dataset_name}/{source_config.source_name}"
                
                tasks.append(
                    ExtractionTask(source_config=source_config, prefix=day_prefix, output_s3_path=output_path, transaction_id=tx_id)
                )
                current_date += timedelta(days=1)
                
        return tasks
