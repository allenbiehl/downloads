import click
from dataset_ageoff.utils.click import LazyGroup

@click.group(
    cls=LazyGroup,
    lazy_subcommands={
        "inventory": "dataset_ageoff.inventory.cli:cli"
    },
)
@click.pass_context
def cli(ctx: click.core.Context):
    """
    Main click executable
    """
    ctx.ensure_object(dict)
