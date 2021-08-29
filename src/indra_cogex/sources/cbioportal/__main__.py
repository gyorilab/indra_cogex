import click
from more_click import verbose_option

from . import CcleCnaProcessor, CcleDrugResponseProcessor, CcleMutationsProcessor


@click.command()
@verbose_option
@click.pass_context
def _main(ctx: click.Context):
    ctx.invoke(CcleCnaProcessor.get_cli())
    ctx.invoke(CcleMutationsProcessor.get_cli())
    ctx.invoke(CcleDrugResponseProcessor.get_cli())


if __name__ == "__main__":
    _main()
