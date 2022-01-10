# -*- coding: utf-8 -*-

"""Run the database indexing."""

import click

from . import index_evidence_on_stmt_hash


@click.command(name="Build extra indexes")
@click.option(
    "--index_evidence_nodes",
    is_flag=True,
    help="Index the Evidence nodes on the stmt_hash property."
)
def main(index_evidence_nodes: bool = False, exist_ok: bool = True):
    """Build indexes on the database."""
    if index_evidence_nodes:
        index_evidence_on_stmt_hash(exist_ok=exist_ok)
    click.secho("Started all requested indexing.", fg="green")
