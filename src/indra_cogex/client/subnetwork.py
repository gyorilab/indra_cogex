from typing import List, Tuple
from indra.statements import Statement
from .queries import get_expressed_genes_in_tissue
from .neo4j_client import Neo4jClient
from ..representation import norm_id, Node, indra_stmts_from_relations


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
    # TODO add BioEntity constraint to source and target,
    #  since all INDRA-like entities should be BioEntities?
    query = """MATCH p=(n1)-[r:indra_rel]->(n2)
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
    genes = get_expressed_genes_in_tissue(client, tissue)
    relevant_genes = {g.grounding() for g in genes} & set(nodes)
    return indra_subnetwork(client, relevant_genes)
