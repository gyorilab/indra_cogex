# -*- coding: utf-8 -*-

"""A collection of functions for indexing on the database."""
from indra_cogex.client.neo4j_client import Neo4jClient


def index_evidence_on_stmt_hash(client: Neo4jClient, exist_ok: bool = False):
    """Index all Evidence nodes on the statement hash

    Parameters
    ----------
    client :
        Neo4jClient instance to the graph database to be indexed
    exist_ok :
        If False, raise an exception if the index already exists. Default: False.
    """
    client.create_single_property_node_index(
        index_name="ev_hash",
        label="Evidence",
        property_name="stmt_hash",
        exist_ok=exist_ok,
    )


def index_indra_rel_on_stmt_hash(client: Neo4jClient):
    """Index all indra_rel relationships on stmt_hash

    Parameters
    ----------
    client :
        Neo4jClient instance to the graph database to be indexed
    """
    client.create_single_property_relationship_index(
        index_name="indra_rel_hash", rel_type="indra_rel", property_name="stmt_hash"
    )
