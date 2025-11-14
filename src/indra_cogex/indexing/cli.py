# -*- coding: utf-8 -*-

"""Run the database indexing."""
from typing import Optional, Tuple

import click


@click.command(name="extra_indexing")
@click.option(
    "--all",
    "all_",
    is_flag=True,
    help="Build all indexes",
)
@click.option(
    "--index-nodes",
    is_flag=True,
    help="Index all nodes on the id property.",
)
@click.option(
    "--index-bioentity-names",
    is_flag=True,
    help="Index BioEntity nodes on the name property. This is useful for "
    "efficiently querying BioEntity nodes by their names.",
)
@click.option(
    "--index-evidence-nodes",
    is_flag=True,
    help="Index the Evidence nodes on the stmt_hash property.",
)
@click.option(
    "--index-indra-relations",
    is_flag=True,
    help="Index the INDRA relations on the stmt_hash property.",
)
@click.option(
    "--index-evidence-embeddings",
    is_flag=True,
    help="Create a vector index on the embedding property of Evidence nodes.",
)
@click.option(
    "--exist-ok",
    is_flag=True,
    help="If set, skip already set indices silently, otherwise an exception "
    "is raised if attempting to set an index that already exists.",
)
def main(
    all_: bool = False,
    index_nodes: bool = False,
    index_bioentity_names: bool = False,
    index_evidence_nodes: bool = False,
    index_indra_relations: bool = False,
    index_evidence_embeddings: bool = False,
    exist_ok: bool = False,
    url: Optional[str] = None,
    auth: Optional[Tuple[str, str]] = None,
):
    """Build indexes on the database."""
    if not any(
        [
            all_,
            index_nodes,
            index_bioentity_names,
            index_evidence_nodes,
            index_indra_relations,
            index_evidence_embeddings,
        ]
    ):
        click.secho(
            "No indexing options provided. Use --all or at least one of the "
            "specific indexing options.",
            fg="red",
        )
        return

    client = _get_client(url, auth)
    if all_ or index_nodes:
        from . import index_nodes_on_id

        click.secho("Indexing all nodes on the id property.", fg="green")
        index_nodes_on_id(client, exist_ok=exist_ok)

    if all_ or index_bioentity_names:
        from . import index_bioentity_nodes_on_name

        click.secho("Indexing BioEntity nodes on the name property.", fg="green")
        index_bioentity_nodes_on_name(client)

    if all_ or index_evidence_nodes:
        from . import index_evidence_on_stmt_hash

        click.secho("Indexing Evidence nodes on the stmt_hash property.", fg="green")
        index_evidence_on_stmt_hash(client, exist_ok=exist_ok)

    if all_ or index_indra_relations:
        from . import index_indra_rel_on_stmt_hash

        click.secho(
            "Indexing INDRA relations on the stmt_hash property.", fg="green"
        )
        index_indra_rel_on_stmt_hash(client)
    if all_ or index_evidence_embeddings:
        from . import create_vector_index_evidence_nodes

        click.secho(
            "Creating vector index on Evidence node embeddings.", fg="green"
        )
        create_vector_index_evidence_nodes(client)
    click.secho("Started all requested indexing.", fg="green")


def _get_client(
    url: Optional[str] = None, auth: Optional[Tuple[str, str]] = None
) -> "Neo4jClient":
    from ..client.neo4j_client import Neo4jClient

    return Neo4jClient(url, auth)
