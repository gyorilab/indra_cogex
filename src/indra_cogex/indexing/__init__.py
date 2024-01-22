# -*- coding: utf-8 -*-

"""A collection of functions for indexing on the database."""
import time

from tqdm import tqdm

from indra_cogex.client.neo4j_client import Neo4jClient


def index_nodes_on_id(client: Neo4jClient, exist_ok: bool = False):
    """Index all nodes on the id property

    Parameters
    ----------
    client :
        Neo4jClient instance to the graph database to be indexed
    exist_ok :
        If False, raise an exception if the index already exists. Default: False.
    """
    # A label has to be provided to build the index, so we have to loop over
    # all labels and build the index for each one.
    for label in tqdm(
        [
            "Evidence",
            "Publication",
            "BioEntity",
            "ClinicalTrial",
            "Patent",
            "ResearchProject",
            "Journal",
            "Publisher",
        ]
    ):
        client.create_single_property_node_index(
            index_name=f"node_id_{label.lower()}",
            label=label,
            property_name="id",
            exist_ok=exist_ok,
        )
        # Wait a bit just to be on the safe side
        time.sleep(0.25)


def index_evidence_on_stmt_hash(client: Neo4jClient, exist_ok: bool = False):
    """Index all Evidence nodes on the stmt_hash property

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
    """Index all indra_rel relationships on stmt_hash property

    Parameters
    ----------
    client :
        Neo4jClient instance to the graph database to be indexed
    """
    client.create_single_property_relationship_index(
        index_name="indra_rel_hash", rel_type="indra_rel", property_name="stmt_hash"
    )
