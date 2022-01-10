# -*- coding: utf-8 -*-

"""A collection of functions for indexing on the database."""

from ..client.neo4j_client import Neo4jClient


def index_evidence_on_stmt_hash(exist_ok: bool = True):
    """Index all Evidence nodes on the statement hash

    Parameters
    ----------
    exist_ok :
        If True, raise an exception if the index already exists.
    """
    client = Neo4jClient()
    client.create_single_property_node_index(
        "ev_hash", "Evidence", "stmt_hash", exist_ok=exist_ok
    )
