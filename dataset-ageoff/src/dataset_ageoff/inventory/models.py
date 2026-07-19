# pylint: disable=missing-module-docstring
from dataclasses import dataclass
from datetime import datetime
import enum

class PeriodType(enum.Enum):
    DAY = enum.auto()
    MONTH = enum.auto()

@dataclass
class DateRangePeriod:
    start_date: datetime | None
    end_date: datetime | None
    period_type: PeriodType

@dataclass
class DatasetDirectory:
    id: str
    name: str
    path: str
    age_off_days: int

@dataclass
class AgeoffPendingJobDetails:
    target: DatasetDirectory
    ageoff_date: datetime
    output_dir: str

@dataclass
class AgeoffPeriodJobDetails:
    target: DatasetDirectory
    period: DateRangePeriod
    output_dir: str

@dataclass
class JobExecutionStatus(enum.Enum):
    SUCCESS = enum.auto()
    FAILURE = enum.auto()
    SKIPPED = enum.auto()

@dataclass
class JobExecutionResult:
    status: JobExecutionStatus
    duration: float = 0.0
    exit_code: int | None = None
    error_message: str | None = None

@dataclass
class InventoryConfig:
    s3_access_key: str
    s3_secret_key: str
    s3_endpoint_override: str
    s3_scheme: str
    s3_force_virtual_addressing: bool
    s3_region: str
    s3_bucket: str
    s3_prefix: str
    max_workers: int
    batch_size: int

