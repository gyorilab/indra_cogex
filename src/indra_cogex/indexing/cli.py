# -*- coding: utf-8 -*-

"""Run the database indexing."""

import click


@click.command(name="Build extra indexes")
@click.option(
    "--index-evidence-nodes",
    is_flag=True,
    help="Index the Evidence nodes on the stmt_hash property."
)
@click.option(
    "--exist-ok",
    is_flag=True,
    help="If set, skip already set indices silently, otherwise an exception "
         "is raised if attempting to set an index that already exists."
)
def main(index_evidence_nodes: bool = False, exist_ok: bool = False):
    """Build indexes on the database."""
    if index_evidence_nodes:
        from . import index_evidence_on_stmt_hash
        click.secho("Indexing Evidence nodes on the stmt_hash property.", fg="green")
        index_evidence_on_stmt_hash(exist_ok=exist_ok)
    click.secho("Started all requested indexing.", fg="green")
