# pylint: disable=missing-module-docstring
from datetime import datetime, timezone
from pathlib import Path

import click
from dataset_ageoff.audit.ageoff_pending_manager import AgeoffPendingManager
from dataset_ageoff.audit.ageoff_period_manager import AgeoffPeriodManager
from dataset_ageoff.audit.models import PeriodType
from dataset_ageoff.clients.dataset_api_client import DatasetApiClient
from dataset_ageoff.report.ageoff_summary_report import DatasetReportReconstitutor

@click.group(name="report")
@click.pass_context
def cli(_ctx: click.core.Context) -> None:
    """
    Main report cli
    """

@cli.command()
@click.pass_context
@click.option(
    "--config", 
    type=click.STRING,
    default=None,
    required=True
)
def create_summary(_ctx: click.core.Context, config: str):
    """
    Create summary report
    """
    client = DatasetApiClient()
    projects = client.get_directories()
    
    config_path = str(Path(config).resolve())    
    reconstitutor = DatasetReportReconstitutor(config_path=config_path)
    
    # Pass Q1 month parameters explicitly
    q1_months = ["01", "02", "03"]
    quarterly_report = reconstitutor.generate_report(year="2026", months=q1_months, datasets=projects)
    
    # Display output matches your exact expected row patterns
    for row in quarterly_report:
        print(f"{row['year']} | {row['month']} | {row['project']} | {row['size']} | {row['count']}")


