from dataclasses import dataclass

@dataclass(frozen=True)
class ExtractionTask:
    """Explicit data model representing a chunk of S3 pathing work."""
    strategy_type: str  # Expected: 'MONTH' or 'DAY'
    prefix: str         # Expected: 'yyyy/mm/' or 'yyyy/mm/dd/'
