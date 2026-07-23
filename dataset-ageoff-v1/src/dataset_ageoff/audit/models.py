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
class AuditConfig:
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

