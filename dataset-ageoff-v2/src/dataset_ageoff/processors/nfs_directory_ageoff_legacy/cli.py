# pylint: disable=missing-module-docstring
from datetime import datetime, timezone
from pathlib import Path

import click
from dataset_ageoff.processors.nfs_directory_ageoff.nfs_directory_ageoff_processor import NfsDirectoryAgeoffProcessor
from dataset_ageoff.processors.nfs_directory_common.models import PeriodType

@click.group(name="nfs-directory-ageoff")
@click.pass_context
def cli(_ctx: click.core.Context) -> None:
    """
    Main audit cli
    """

@cli.command()
@click.pass_context
@click.option(
    "--config", 
    type=click.STRING,
    default=None,
    required=True
)
@click.option(
    "--start-date", 
    type=click.STRING,
    default=None,
    required=True
)
@click.option(
    "--period-type", 
    type=click.Choice(PeriodType, case_sensitive=False),
    default=PeriodType.MONTH,
    required=True
)
def create_period(_ctx: click.core.Context, config: str, start_date: str, period_type: PeriodType):
    """
    Create ageoff period audit
    """
    start_timestamp = datetime.fromisoformat(start_date)
    start_timestamp = start_timestamp.replace(tzinfo=timezone.utc)
    config_path = str(Path(config).resolve())
    NfsDirectoryAgeoffProcessor(config_path=config_path).execute(
        start_date=start_timestamp, period_type=period_type
    )
