# -*- coding: utf-8 -*-

"""A collection of functions for indexing on the database."""

from ..client.neo4j_client import Neo4jClient


def index_evidence_on_stmt_hash(exist_ok: bool = False):
    """Index all Evidence nodes on the statement hash

    Parameters
    ----------
    exist_ok :
        If False, raise an exception if the index already exists. Default: False.
    """
    client = Neo4jClient()
    client.create_single_property_node_index(
        "ev_hash", "Evidence", "stmt_hash", exist_ok=exist_ok
    )


def index_indra_rel_on_stmt_hash():
    """Index all indra_rel relationships on stmt_hash"""
    client = Neo4jClient()
    client.create_single_property_node_index(
        index_name="indra_rel_hash", label="indra_rel", property_name="stmt_hash"
    )
