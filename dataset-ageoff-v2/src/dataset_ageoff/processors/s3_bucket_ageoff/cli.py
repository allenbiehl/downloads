import click
from dataset_ageoff.common.clients.dataset_api_client import DatasetApiClient
from dataset_ageoff.processors.s3_bucket_ageoff.ledger import LocalProvenanceLedger, LocalProvenanceLedgerConfig
from dataset_ageoff.processors.s3_bucket_ageoff.orchestrator import KubernetesLifecycleOrchestrator, KubernetesLifecycleOrchestratorConfig
from dataset_ageoff.processors.s3_bucket_ageoff.purger import S3DatasetPurger, S3DatasetPurgerConfig
from dataset_ageoff.common.clients.s3_client_factory import S3ClientFactory, S3ClientFactoryConfig
from dataset_ageoff.processors.s3_bucket_ageoff.scanner import S3DatasetScanner

@click.group(name="s3-bucket-ageoff")
@click.pass_context
def cli(_ctx: click.core.Context) -> None:
    """
    Main s3_bucket_ageoff cli
    """

@cli.command()
@click.pass_context
def execute(_ctx: click.core.Context):

    source_configs = DatasetApiClient().get_s3_buckets()

    orchestrator_config = KubernetesLifecycleOrchestratorConfig(max_workers=16, dry_run=False)
    factory_config = S3ClientFactoryConfig(
        endpoint_url="http://localhost:9020",
        aws_access_key_id="admin",
        aws_secret_access_key="password",
        max_workers=orchestrator_config.max_workers
    )
    client_factory = S3ClientFactory(config=factory_config)
    s3_client = client_factory.create_client()

    ledger_config = LocalProvenanceLedgerConfig(
        db_mount_path="./.data/s3_bucket_ageoff/provenance.db"
    )
    ledger = LocalProvenanceLedger(config=ledger_config)
    s3_scanner = S3DatasetScanner(s3_client=s3_client, ledger=ledger)

    purger_config = S3DatasetPurgerConfig(dry_run=orchestrator_config.dry_run)
    s3_purger = S3DatasetPurger(s3_client=s3_client, config=purger_config)

    orchestrator = KubernetesLifecycleOrchestrator(
        scanner=s3_scanner,
        purger=s3_purger,
        s3_client=s3_client,
        ledger=ledger,
        config=orchestrator_config
    )
    
    orchestrator.process_all_datasets(dataset_configs=source_configs)