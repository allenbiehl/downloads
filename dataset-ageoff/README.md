# Dataset Age-Off

## Local Development

1. Start Minio

```bash
docker compose up minio
```

1. Install editable mode

```bash
pip install -e .
```

2. Create pending audit

```bash
dataset-ageoff audit create-pending \
  --config resources/ageoff-pending-config.yaml
```

2. Create period audit

```bash
dataset-ageoff audit create-period \
  --config resources/ageoff-period-config.yaml \
  --start-date "2026-01-01" \
  --period-type month

dataset-ageoff audit create-period \
  --config resources/ageoff-period-config.yaml \
  --start-date "2026-02-01" \
  --period-type month
  
dataset-ageoff audit create-period \
  --config resources/ageoff-period-config.yaml \
  --start-date "2026-03-01" \
  --period-type month

dataset-ageoff audit create-period \
  --config resources/ageoff-period-config.yaml \
  --start-date "2026-04-01" \
  --period-type month

dataset-ageoff audit create-period \
  --config resources/ageoff-period-config.yaml \
  --start-date "2026-05-01" \
  --period-type month

dataset-ageoff audit create-period \
  --config resources/ageoff-period-config.yaml \
  --start-date "2026-06-01" \
  --period-type month

dataset-ageoff audit create-period \
  --config resources/ageoff-period-config.yaml \
  --start-date "2026-07-01" \
  --period-type month
```

2. Create summary report

```bash
dataset-ageoff report create-summary \
  --config resources/ageoff-period-config.yaml
```

## fastapi

```
from fastapi import APIRouter, Depends, Query, HTTPException
from typing import List, Optional
from models import AuditConfig, DatasetDirectory
from reconstitutor import DatasetReportReconstitutor

router = APIRouter(prefix="/reports", tags=["audit"])

# Mock function representing your existing repository database discovery hook
def get_dataset_directories() -> List[DatasetDirectory]:
    return [
        DatasetDirectory(id=1001, name="Project_A", path="/Projects/A", age_off_days=0),
        DatasetDirectory(id=1002, name="Project_B", path="/Projects/B", age_off_days=0),
        DatasetDirectory(id=1003, name="Project_C", path="/Projects/C", age_off_days=0)
    ]

# Dependency injection provider to load your central AuditConfig configurations
def get_audit_config() -> AuditConfig:
    # Replace this with your actual FileConfigLoader wrapper loading routine
    return AuditConfig(
        s3_access_key="minio_admin",
        s3_secret_key="minio_secret",
        s3_endpoint_override="localhost:9000",
        s3_scheme="http",
        s3_force_virtual_addressing=False,
        s3_region="us-east-1",
        s3_bucket="dataset-audit",
        s3_prefix="ageoff/period",
        max_workers=4,
        batch_size=300000
    )

@router.get("/quarterly", response_model=List[dict])
async def get_quarterly_dataset_report(
    year: str = Query(..., description="Target execution year (e.g. 2026)"),
    months: List[str] = Query(..., description="List of target months (e.g. ['01', '02', '03'])"),
    config: AuditConfig = Depends(get_audit_config)
):
    """API endpoint to aggregate universal summary configurations across multiple projects and months."""
    try:
        # 1. Gather your targeted dataset project configurations
        active_datasets = get_dataset_directories()
        
        # 2. Instantiate the reconstitutor with your pre-defined base path prefix
        reconstitutor = DatasetReportReconstitutor(config, base_prefix="ageoff/period")
        
        # 3. Generate the flat matrix list directly
        report_data = reconstitutor.generate_report(
            year=year, 
            months=months, 
            datasets=active_datasets
        )
        
        # 4. Return directly - FastAPI converts the native list of dicts to a JSON array automatically
        return report_data
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to compile dataset report matrix: {str(e)}")

```