# pylint: disable=missing-module-docstring
from pathlib import Path

import click
from dataset_ageoff.common.clients.dataset_api_client import DatasetApiClient
from dataset_ageoff.reports.summary_report import SummaryReport

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
    source_configs = client.get_dataset_sources()

    config_path = str(Path(config).resolve())
    report = SummaryReport(config_path=config_path)

    months = ["04", "05", "06"]
    results = report.create(year="2025", months=months, source_configs=source_configs)

    for row in results:
        print(f"{row['year']} | {row['month']} | {row['dataset']} | {row['source']} |{row['size']} | {row['count']}")
