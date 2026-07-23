import click
from dataset_ageoff.common.utils.click import LazyGroup

@click.group(
    cls=LazyGroup,
    lazy_subcommands={
        "nfs-directory-ageoff": "dataset_ageoff.processors.nfs_directory_ageoff.cli:cli",
        "nfs-directory-pending": "dataset_ageoff.processors.nfs_directory_pending.cli:cli",        
        "s3-bucket-ageoff": "dataset_ageoff.processors.s3_bucket_ageoff.cli:cli",
        "report": "dataset_ageoff.reports.cli:cli"
    },
)
@click.pass_context
def cli(ctx: click.core.Context):
    """
    Main click executable
    """
    ctx.ensure_object(dict)
