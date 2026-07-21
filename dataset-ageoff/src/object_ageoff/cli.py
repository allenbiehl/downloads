from datetime import datetime, timedelta
import os
from typing import List

import click
import random
from object_ageoff.ledger import LocalProvenanceLedger
from object_ageoff.orchestrator import KubernetesLifecycleOrchestrator
from object_ageoff.s3_client_factory import S3ClientFactory
from object_ageoff.s3_dataset_scanner import S3DatasetScanner
from object_ageoff.test_generator import S3StructuralTestDataGenerator
from object_ageoff.timeline import LifecycleTimelineCalculator

# @click.group(
#     cls=LazyGroup,
#     lazy_subcommands={
#         "audit": "dataset_ageoff.audit.cli:cli",
#         "report": "dataset_ageoff.report.cli:cli"
#     },
# )
# @click.pass_context
# def cli(ctx: click.core.Context):
#     """
#     Main click executable
#     """
#     ctx.ensure_object(dict)


@click.group(name="object")
@click.pass_context
def cli(_ctx: click.core.Context) -> None:
    """
    Main object_ageoff cli
    """

@cli.command()
@click.pass_context
def seed(_ctx: click.core.Context):

    # Point this to your test MinIO cluster, NetApp device, or local S3 mock endpoint
    TARGET_ENDPOINT: str = "http://localhost:9020"
    ACCESS_KEY: str = "admin"
    SECRET_KEY: str = "password"
    
    generator = S3StructuralTestDataGenerator(
        endpoint_url=TARGET_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        max_workers=64
    )
    
    now: datetime = datetime.now()
    
    # -------------------------------------------------------------
    # SCENARIO 1: The Long-Term Archive (Starts in 2008)
    # -------------------------------------------------------------
    # Pure random dates stretching back 18 years
    b1_dates: List[datetime] = [
        now - timedelta(days=random.randint(0, 18 * 365)) 
        for _ in range(2000)
    ]
    generator.populate_bucket_with_timeline("enterprise-dataset-bucket-1", b1_dates)
    
    # -------------------------------------------------------------
    # SCENARIO 2: The Modern App (Starts in 2019)
    # -------------------------------------------------------------
    # Random dates strictly capped inside the last 7 years. 2000-2018 will be completely empty.
    b2_dates: List[datetime] = [
        now - timedelta(days=random.randint(0, 7 * 365)) 
        for _ in range(2000)
    ]
    generator.populate_bucket_with_timeline("enterprise-dataset-bucket-2", b2_dates)
    
    # -------------------------------------------------------------
    # SCENARIO 3: The Broken Timeline / Backfill Chaos (Only 2012 + 2023+)
    # -------------------------------------------------------------
    b3_dates: List[datetime] = []
    
    # Injection A: Cluster files exclusively inside the year 2012 (approx 5100 days ago)
    for _ in range(200):
        random_day_in_2012 = now - timedelta(days=random.randint(5110, 5475))
        b3_dates.append(random_day_in_2012)
        
    # Injection B: Regular modern traffic starting 3 years ago (2023 onwards)
    for _ in range(1000):
        modern_day = now - timedelta(days=random.randint(0, 3 * 365))
        b3_dates.append(modern_day)
        
    generator.populate_bucket_with_timeline("enterprise-dataset-bucket-3", b3_dates)
    
    print("\nData provisioning complete. Your orchestrator script will now discover highly distinct ranges.")


@cli.command()
@click.pass_context
def execute(_ctx: click.core.Context):
    my_35_buckets: List[str] = [f"enterprise-dataset-bucket-{i}" for i in range(1, 4)]
    WORKER_POOL_SIZE: int = 16
    AUDIT_BUCKET: str = os.getenv("COMPLIANCE_AUDIT_BUCKET", "corporate-lifecycle-audit-reporting")
    KUBERNETES_BLOCK_MOUNT: str = os.getenv("LEDGER_DB_PATH", "/Users/evanbiehl/Projects/dataset-ageoff/.data/storagegrid-ledger/provenance.db")

    # Instantiate Factory
    client_factory = S3ClientFactory(
        endpoint_url="http://localhost:9020",
        aws_access_key_id="admin",
        aws_secret_access_key="password",
        max_workers=WORKER_POOL_SIZE
    )
    scanner_client = client_factory.create_client()
    orchestrator_client = client_factory.create_client()

    # Initialize Engine Components
    sqlite_ledger = LocalProvenanceLedger(db_mount_path=KUBERNETES_BLOCK_MOUNT)
    timeline_engine = LifecycleTimelineCalculator(age_off_days=4000)
    s3_scanner = S3DatasetScanner(s3_client=scanner_client, ledger=sqlite_ledger)

    # Execute Main Controller
    orchestrator = KubernetesLifecycleOrchestrator(
        scanner=s3_scanner,
        orchestrator_s3_client=orchestrator_client,
        ledger=sqlite_ledger,
        timeline=timeline_engine,
        audit_bucket=AUDIT_BUCKET,
        max_workers=WORKER_POOL_SIZE,
        dry_run=True
    )

    orchestrator.process_all_datasets(bucket_list=my_35_buckets)
