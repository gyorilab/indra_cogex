# -*- coding: utf-8 -*-

"""
Run the INDRA database processor using ``python -m indra_cogex.sources.indra_db``.
To fetch the latest data from indra_db, use option --sync_data
"""
import click
from more_click import verbose_option
from .export_assembly import export_assembly
from . import DbProcessor, EvidenceProcessor


@click.command()
@verbose_option
@click.option(
    "--sync_data",
    is_flag=True,
    help="Download latest processed data of indra_db from S3 and generate grounded and unique statements.",
)
@click.pass_context
def _main(ctx: click.Context, sync_data: bool):
    if sync_data:
        click.secho("Getting data from S3 and processing with export_assembly.py", fg="yellow")
        export_assembly()
    ctx.invoke(DbProcessor.get_cli())
    ctx.invoke(EvidenceProcessor.get_cli())


if __name__ == "__main__":
    _main()
