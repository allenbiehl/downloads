# pylint: disable=missing-module-docstring
from pathlib import Path

import click
from dataset_ageoff.processors.nfs_directory_pending.nfs_directory_pending_procesor import NfsDirectoryPendingProcessor

@click.group(name="nfs-directory-pending")
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
def execute(_ctx: click.core.Context, config: str):
    """
    Create ageoff pending audit
    """
    config_path = str(Path(config).resolve())
    NfsDirectoryPendingProcessor(config_path=config_path).execute()
