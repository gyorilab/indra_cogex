# -*- coding: utf-8 -*-

"""Run the INDRA database processor using ``python -m indra_cogex.sources.indra_db``."""
import click
from more_click import verbose_option

from . import DbProcessor, EvidenceProcessor


@click.command()
@click.option(
    "--add_jsons", is_flag=True, help="Add statements JSONs in indra_rel relation data"
)
@verbose_option
@click.pass_context
def _main(ctx: click.Context, add_jsons: bool):
    ctx.invoke(DbProcessor.get_cli()(add_jsons))
    ctx.invoke(EvidenceProcessor.get_cli())


if __name__ == "__main__":
    _main()
