# pylint: disable=missing-module-docstring
from datetime import datetime, timezone
from pathlib import Path

import click
from dataset_ageoff.inventory.ageoff_pending_manager import AgeoffPendingManager
from dataset_ageoff.inventory.ageoff_period_manager import AgeoffPeriodManager
from dataset_ageoff.inventory.models import PeriodType

@click.group(name="inventory")
@click.pass_context
def cli(_ctx: click.core.Context) -> None:
    """
    Main inventory cli
    """

@cli.command()
@click.pass_context
@click.option(
    "--config", 
    type=click.STRING,
    default=None,
    required=True
)
def create_pending(_ctx: click.core.Context, config: str):
    """
    Create ageoff pending inventory
    """
    config_path = str(Path(config).resolve())
    AgeoffPendingManager(config_path=config_path).execute()

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
    Create ageoff period inventory
    """
    start_timestamp = datetime.fromisoformat(start_date)
    start_timestamp = start_timestamp.replace(tzinfo=timezone.utc)
    config_path = str(Path(config).resolve())
    AgeoffPeriodManager(config_path=config_path).execute(
        start_date=start_timestamp, period_type=period_type
    )
