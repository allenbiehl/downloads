# pylint: disable=missing-module-docstring
import click

from dataset_ageoff.common.clients.dataset_api_client import DatasetApiClient
from dataset_ageoff.common.clients.s3_client_factory import S3ClientFactory, S3ClientFactoryConfig
from dataset_ageoff.processors.nfs_directory_ageoff.ledger import LocalNfsProvenanceLedger, LocalNfsProvenanceLedgerConfig
from dataset_ageoff.processors.nfs_directory_ageoff.orchestrator import ContinuousNfsLifecycleOrchestrator, ContinuousNfsLifecycleOrchestratorConfig
from dataset_ageoff.processors.nfs_directory_ageoff.purger import NfsDatasetPurger, NfsDatasetPurgerConfig
from dataset_ageoff.processors.nfs_directory_ageoff.scanner import NfsDatasetScanner

@click.group(name="nfs-directory-ageoff")
@click.pass_context
def cli(_ctx: click.core.Context) -> None:
    """
    Main audit cli
    """

@cli.command()
@click.pass_context
# @click.option(
#     "--config", 
#     type=click.STRING,
#     default=None,
#     required=True
# )
def execute(_ctx: click.core.Context) -> None:
    """
    Create ageoff period audit
    """
    source_configs = DatasetApiClient().get_nfs_directories()

    orchestrator_config = ContinuousNfsLifecycleOrchestratorConfig(max_workers=16, dry_run=False)
    factory_config = S3ClientFactoryConfig(
        endpoint_url="http://localhost:9020",
        aws_access_key_id="admin",
        aws_secret_access_key="password",
        max_workers=orchestrator_config.max_workers
    )
    client_factory = S3ClientFactory(config=factory_config)
    s3_client = client_factory.create_client()

    ledger_config = LocalNfsProvenanceLedgerConfig(
        db_mount_path="./.data/nfs_directory_ageoff/provenance.db"
    )
    ledger = LocalNfsProvenanceLedger(config=ledger_config)
    scanner = NfsDatasetScanner(ledger=ledger)

    purger_config = NfsDatasetPurgerConfig(dry_run=orchestrator_config.dry_run)
    purger = NfsDatasetPurger(config=purger_config)

    orchestrator = ContinuousNfsLifecycleOrchestrator(
        scanner=scanner,
        purger=purger,
        s3_client=s3_client,
        ledger=ledger,
        config=orchestrator_config
    )
    
    orchestrator.process_all_datasets(dataset_configs=source_configs)
