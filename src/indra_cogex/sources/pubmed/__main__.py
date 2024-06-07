"""Run the PubMed processor using ``python -m indra_cogex.sources.pubmed``."""
import click
from more_click import verbose_option

from . import JournalProcessor, PublicationProcessor


@click.command()
@verbose_option
@click.pass_context
def _main(ctx: click.Context):
    ctx.invoke(JournalProcessor.get_cli())
    ctx.invoke(PublicationProcessor.get_cli())


if __name__ == "__main__":
    _main()
