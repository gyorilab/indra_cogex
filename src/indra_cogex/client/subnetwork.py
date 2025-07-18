"""Queries that generate statement subnetworks."""

import json
from typing import Any, List, Tuple, Union, Dict
import logging

from indra.statements import Statement

from .neo4j_client import Neo4jClient, autoclient
from .queries import get_genes_for_go_term, get_genes_in_tissue, get_stmts_meta_for_stmt_hashes
from ..representation import Relation, indra_stmts_from_relations, norm_id

logger = logging.getLogger(__name__)

__all__ = [
    "indra_subnetwork",
    "indra_subnetwork_relations",
    "indra_mediated_subnetwork",
    "indra_subnetwork_tissue",
    "indra_subnetwork_go",
]


@autoclient()
def indra_subnetwork_relations(
    nodes: List[Tuple[str, str]], *, client: Neo4jClient, include_db_evidence: bool = True
) -> List[Relation]:
    """Return the subnetwork induced by the given nodes as a set of Relations.

    Parameters
    ----------
    client :
        The Neo4j client.
    nodes :
        The nodes to query.
    include_db_evidence :
        Whether to include statements with database evidence.

    Returns
    -------
    :
        The subnetwork induced by the given nodes represented as Relation
        objects.
    """
    nodes_str = ", ".join(["'%s'" % norm_id(*node) for node in nodes])
    query = f"""MATCH p=(n1:BioEntity)-[r:indra_rel]->(n2:BioEntity)
               WHERE n1.id IN [{nodes_str}]
               AND n2.id IN [{nodes_str}]
               AND n1.id <> n2.id
               {'' if include_db_evidence else 'AND NOT r.has_database_evidence'}
               RETURN p"""
    return client.query_relations(query)


@autoclient()
def indra_subnetwork_meta(
    nodes: List[Tuple[str, str]], *, client: Neo4jClient
) -> List[List[Any]]:
    """Return the subnetwork induced by the given nodes as a list of metadata
    on relations.

    Parameters
    ----------
    nodes :
        The nodes to query.
    client :
        The Neo4j client.

    Returns
    -------
    :
        The subnetwork induced by the given nodes represented as a list of
        metadata on relations. The elements of each list are:
        CURIE of source node, CURIE of target node, statement type,
        statement hash, source counts.
    """
    nodes_str = ", ".join(["'%s'" % norm_id(*node) for node in nodes])
    query = """MATCH p=(n1:BioEntity)-[r:indra_rel]->(n2:BioEntity)
            WHERE n1.id IN [%s]
            AND n2.id IN [%s]
            AND n1.id <> n2.id
            RETURN n1.id, n2.id, r.stmt_type, r.stmt_hash, r.source_counts""" % (
        nodes_str,
        nodes_str,
    )
    res = client.query_tx(query)
    # Turn source counts into dicts
    res = [r[:-1] + [json.loads(r[-1])] for r in res]
    return res


@autoclient()
def indra_subnetwork(
    nodes: List[Tuple[str, str]],
    *,
    client: Neo4jClient,
    include_db_evidence: bool = True,
    order_by_ev_count: bool = False,
    return_source_counts: bool = False,
) -> Union[List[Statement], Tuple[List[Statement], Dict[int, Dict[str, int]]]]:
    """Return the INDRA Statement subnetwork induced by the given nodes.

    Parameters
    ----------
    client :
        The Neo4j client.
    nodes :
        The nodes to query.
    include_db_evidence :
        Whether to include statements with database evidence.
    order_by_ev_count :
        Whether to order the statements by evidence count in the query.
    return_source_counts :
        Whether to return source counts as well as statements.

    Returns
    -------
    :
        The subnetwork induced by the given nodes.
    """
    rels = indra_subnetwork_relations(
        nodes=nodes,
        client=client,
        include_db_evidence=include_db_evidence
    )
    stmts = indra_stmts_from_relations(rels, order_by_ev_count=order_by_ev_count)
    if return_source_counts:
        source_counts = {
            r.data["stmt_hash"]: json.loads(r.data["source_counts"]) for r in rels
        }
        return stmts, source_counts
    return stmts


@autoclient()
def indra_mediated_subnetwork(
    nodes: List[Tuple[str, str]],
    *,
    client: Neo4jClient,
    order_by_ev_count: bool = False,
) -> List[Statement]:
    """Return the INDRA Statement subnetwork induced pairs of statements
    between the given nodes.

    For example, if gene A and gene B are given as the query, find statements
    mediated by X such that A -> X -> B.

    Parameters
    ----------
    client :
        The Neo4j client.
    nodes :
        The nodes to query.
    order_by_ev_count :
        Whether to order the statements by evidence count in descending order.

    Returns
    -------
    :
        The subnetwork induced by the given nodes.
    """
    return get_two_step_subnetwork(
        client=client,
        nodes=nodes,
        first_forward=True,
        second_forward=True,
        order_by_ev_count=order_by_ev_count
    )


@autoclient()
def indra_shared_downstream_subnetwork(
    nodes: List[Tuple[str, str]],
    *,
    client: Neo4jClient,
    order_by_ev_count: bool = False,
) -> List[Statement]:
    """Return the INDRA Statement subnetwork induced by shared downstream targets
    of nodes in the query.

    For example, if gene A and gene B are given as the query, find statements
    to shared downstream entity X such that A -> X <- B.

    Parameters
    ----------
    client :
        The Neo4j client.
    nodes :
        The nodes to query.
    order_by_ev_count :
        Whether to order the statements by evidence count in descending order.

    Returns
    -------
    :
        The subnetwork induced by the given nodes.
    """
    return get_two_step_subnetwork(
        client=client,
        nodes=nodes,
        first_forward=True,
        second_forward=False,
        order_by_ev_count=order_by_ev_count
    )


@autoclient()
def indra_shared_upstream_subnetwork(
    nodes: List[Tuple[str, str]],
    *,
    client: Neo4jClient,
    order_by_ev_count: bool = False,
) -> List[Statement]:
    """Return the INDRA Statement subnetwork induced by shared upstream controllers
    of nodes in the query.

    For example, if gene A and gene B are given as the query, find statements
    to shared upstream entity X such that A <- X -> B.

    Parameters
    ----------
    client :
        The Neo4j client.
    nodes :
        The nodes to query.
    order_by_ev_count :
        Whether to order the statements by evidence count in descending order.

    Returns
    -------
    :
        The subnetwork induced by the given nodes.
    """
    return get_two_step_subnetwork(
        client=client,
        nodes=nodes,
        first_forward=False,
        second_forward=True,
        order_by_ev_count=order_by_ev_count
    )


def get_two_step_subnetwork(
    *,
    nodes: List[Tuple[str, str]],
    client: Neo4jClient,
    first_forward: bool = True,
    second_forward: bool = True,
    order_by_ev_count: bool = False,
) -> List[Statement]:
    """Return the INDRA Statement subnetwork induced by paths of length
    two between nodes A and B in a query with intermediate nodes X such
    that paths look like A-X-B.

    Parameters
    ----------
    nodes :
        The nodes to query (A and B are one of these nodes in
        the following examples).
    client :
        The Neo4j client.
    first_forward:
        If true, query A->X otherwise query A<-X
    second_forward:
        If true, query X->B otherwise query X<-B
    order_by_ev_count :
        Whether to order the statements by evidence count in descending order.

    Returns
    -------
    :
        The INDRA statement subnetwork induced by the query
    """
    nodes_str = ", ".join(["'%s'" % norm_id(*node) for node in nodes])
    f1, f2 = ("-", "->") if first_forward else ("<-", "-")
    s1, s2 = ("-", "->") if second_forward else ("<-", "-")
    query = f"""\
        MATCH p=(n1:BioEntity){f1}[r1:indra_rel]{f2}(n3:BioEntity){s1}[r2:indra_rel]{s2}(n2:BioEntity)
        WHERE
            n1.id IN [{nodes_str}]
            AND n2.id IN [{nodes_str}]
            AND n1.id <> n2.id
            AND NOT n3 IN [{nodes_str}]
        RETURN p
    """
    return _paths_to_stmts(client=client, query=query, order_by_ev_count=order_by_ev_count)


def _paths_to_stmts(
    *,
    client: Neo4jClient,
    query: str,
    order_by_ev_count: bool = False
) -> List[Statement]:
    """Generate INDRA statements from a query that returns paths of length > 1."""
    return indra_stmts_from_relations(
        (
            relation for path in client.query_tx(query)
            for relation in client.neo4j_to_relations(path[0])
        ),
        order_by_ev_count=order_by_ev_count,
    )


@autoclient()
def indra_subnetwork_tissue(
    nodes: List[Tuple[str, str]],
    tissue: Tuple[str, str],
    *,
    client: Neo4jClient,
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
    relevant_genes = {g.grounding() for g in genes} & {tuple(nc) for nc in nodes}
    return indra_subnetwork(list(relevant_genes), client=client)


@autoclient()
def indra_subnetwork_go(
    go_term: Tuple[str, str],
    *,
    client: Neo4jClient,
    include_indirect: bool = False,
    mediated: bool = False,
    upstream_controllers: bool = False,
    downstream_targets: bool = False,
    include_db_evidence: bool = True,
    order_by_ev_count: bool = False,
    return_source_counts: bool = False,
) -> Union[List[Statement], Tuple[List[Statement], Dict[int, Dict[str, int]]]]:
    """Return the INDRA Statement subnetwork induced by the given GO term.

    Parameters
    ----------
    include_db_evidence :
        Whether to include database evidence or not.
    go_term :
        The GO term to query. Example: ``("GO", "GO:0006915")``
    client :
        The Neo4j client.
    include_indirect :
        Should ontological children of the given GO term
        be queried as well? Defaults to False.
    mediated:
        Should relations A->X->B be included for X not associated
        to the given GO term? Defaults to False.
    upstream_controllers:
        Should relations A<-X->B be included for upstream controller
        X not associated to the given GO term? Defaults to False.
    downstream_targets:
        Should relations A->X<-B be included for downstream target
        X not associated to the given GO term? Defaults to False.
    order_by_ev_count:
        Should the statements be ordered by evidence count? Defaults to False.
    return_source_counts:
        Whether to return source counts as well as statements.

    Returns
    -------
    :
        The INDRA statement subnetwork induced by GO term.
    """
    genes = get_genes_for_go_term(
        client=client, go_term=go_term, include_indirect=include_indirect
    )
    nodes = {g.grounding() for g in genes}
    rv = indra_subnetwork(
        client=client,
        nodes=nodes,
        include_db_evidence=include_db_evidence,
        order_by_ev_count=order_by_ev_count,
    )
    if mediated:
        rv.extend(
            indra_mediated_subnetwork(client=client,
                                      nodes=nodes,
                                      include_db_evidence=include_db_evidence,
                                      order_by_ev_count=order_by_ev_count)
        )
    if upstream_controllers:
        rv.extend(
            indra_shared_upstream_subnetwork(client=client,
                                             nodes=nodes,
                                             include_db_evidence=include_db_evidence,
                                             order_by_ev_count=order_by_ev_count)
        )
    if downstream_targets:
        rv.extend(
            indra_shared_downstream_subnetwork(client=client,
                                               nodes=nodes,
                                               include_db_evidence=include_db_evidence,
                                               order_by_ev_count=order_by_ev_count)
        )
    # No deduplication of statements based on the union of
    # the queries should be necessary since each are disjoint
    if return_source_counts:
        stmt_hashes = {stmt.get_hash() for stmt in rv}
        rels = get_stmts_meta_for_stmt_hashes(client=client, stmt_hashes=stmt_hashes)
        source_counts = {
            r.data["stmt_hash"]: json.loads(r.data["source_counts"]) for r in rels
        }
        return rv, source_counts
    return rv
