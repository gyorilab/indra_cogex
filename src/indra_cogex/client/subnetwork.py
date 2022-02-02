from typing import List, Tuple

from indra.statements import Statement

from .neo4j_client import Neo4jClient
from .queries import get_genes_in_tissue
from ..representation import indra_stmts_from_relations, norm_id

__all__ = [
    "indra_subnetwork",
    "indra_subnetwork_tissue",
]


def indra_subnetwork(
    client: Neo4jClient, nodes: List[Tuple[str, str]]
) -> List[Statement]:
    """Return the INDRA Statement subnetwork induced by the given nodes.

    Parameters
    ----------
    client :
        The Neo4j client.
    nodes :
        The nodes to query.

    Returns
    -------
    :
        The subnetwork induced by the given nodes.
    """
    nodes_str = ", ".join(["'%s'" % norm_id(*node) for node in nodes])
    query = """MATCH p=(n1:BioEntity)-[r:indra_rel]->(n2:BioEntity)
            WHERE n1.id IN [%s]
            AND n2.id IN [%s]
            AND n1.id <> n2.id
            RETURN p""" % (
        nodes_str,
        nodes_str,
    )
    rels = [client.neo4j_to_relation(p[0]) for p in client.query_tx(query)]
    stmts = indra_stmts_from_relations(rels)
    return stmts


def indra_subnetwork_tissue(
    client: Neo4jClient, nodes: List[Tuple[str, str]], tissue: Tuple[str, str]
) -> List[Statement]:
    """Return the INDRA Statement subnetwork induced by the given nodes and expressed in the given tissue.

    Parameters
    ----------
    client :
        The Neo4j client.
    nodes :
        The nodes to query.
    tissue :
        The tissue to query.

    Returns
    -------
    :
        The subnetwork induced by the given nodes and expressed in the given tissue.
    """
    genes = get_genes_in_tissue(client=client, tissue=tissue)
    relevant_genes = {g.grounding() for g in genes} & set(nodes)
    return indra_subnetwork(client, relevant_genes)
