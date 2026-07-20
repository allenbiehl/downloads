import click
from dataset_ageoff.utils.click import LazyGroup

@click.group(
    cls=LazyGroup,
    lazy_subcommands={
        "audit": "dataset_ageoff.audit.cli:cli",
        "report": "dataset_ageoff.report.cli:cli"
    },
)
@click.pass_context
def cli(ctx: click.core.Context):
    """
    Main click executable
    """
    ctx.ensure_object(dict)
