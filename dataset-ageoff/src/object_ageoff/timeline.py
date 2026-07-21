from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import List

from object_ageoff.models import ExtractionTask

class LifecycleTimelineCalculator:
    """Responsibility: Manages calendar offsets and target prefix generation."""
    
    _cutoff_date: datetime

    def __init__(self, age_off_days: int = 400) -> None:
        self._cutoff_date = datetime.now() - timedelta(days=age_off_days)

    @property
    def cutoff_date(self) -> datetime:
        """Exposes the internal calculation bound."""
        return self._cutoff_date

    def generate_hybrid_prefixes(self, start_year: int) -> List[ExtractionTask]:
        """Combines structural monthly prefixes and fallback daily blocks with start bounds enforcement."""
        tasks: List[ExtractionTask] = []
        current_date: datetime = datetime(year=start_year, month=1, day=1)
        
        # BUCKET DATASET GUARD: If the earliest file found in the dataset 
        # is newer than our 4,000-day retention cutoff, no data has expired.
        if current_date > self._cutoff_date:
            return tasks # Return an empty list cleanly to skip execution processing

        # 1. Structural Monthly Coarse Chunks
        while current_date.year < self._cutoff_date.year or current_date.month < self._cutoff_date.month:
            tasks.append(
                ExtractionTask(strategy_type='MONTH', prefix=current_date.strftime("%Y/%m/"))
            )
            current_date += relativedelta(months=1)
            
        # 2. Daily Fine-Grained Blocks for the Split Transition Month
        while current_date <= self._cutoff_date:
            tasks.append(
                ExtractionTask(strategy_type='DAY', prefix=current_date.strftime("%Y/%m/%d/"))
            )
            current_date += timedelta(days=1)
            
        return tasks
