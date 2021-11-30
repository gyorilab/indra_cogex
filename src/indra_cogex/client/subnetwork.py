from typing import List, Tuple
from .queries import get_expressed_genes_in_tissue
from .neo4j_client import Neo4jClient
from ..representation import norm_id, Node


def indra_subnetwork(client: Neo4jClient, nodes: List[Tuple[str, str]]) -> List[Node]:
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
    query = """MATCH p=(n1)-[r:indra_rel]->(n2)
            WHERE n1.id IN [%s]
            AND n2.id IN [%s]
            AND n1.id <> n2.id
            RETURN r""" % (
        nodes_str,
        nodes_str,
    )
    res = client.query_tx(query)
    return res


def indra_subnetwork_tissue(
    client: Neo4jClient, nodes: List[Tuple[str, str]], tissue: Tuple[str, str]
):
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
    genes = get_expressed_genes_in_tissue(client, tissue)
    relevant_genes = {g.grounding() for g in genes} & set(nodes)
    return indra_subnetwork(client, relevant_genes)
