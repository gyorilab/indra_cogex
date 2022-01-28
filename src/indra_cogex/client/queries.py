import json
import logging
from collections import Counter, defaultdict
from typing import Dict, Iterable, List, Optional, Set, Tuple, Union

import networkx as nx
from indra.statements import Evidence, Statement, stmts_from_json, Agent

from .neo4j_client import Neo4jClient, autoclient
from ..representation import Node,Relation,  indra_stmts_from_relations, norm_id

logger = logging.getLogger(__name__)

__all__ = [
    "get_genes_in_tissue",
    "get_tissues_for_gene",
    "is_gene_in_tissue",
    "get_go_terms_for_gene",
    "get_genes_for_go_term",
    "is_go_term_for_gene",
    "get_trials_for_drug",
    "get_trials_for_disease",
    "get_drugs_for_trial",
    "get_diseases_for_trial",
    "get_pathways_for_gene",
    "get_shared_pathways_for_genes",
    "get_genes_for_pathway",
    "is_gene_in_pathway",
    "get_side_effects_for_drug",
    "get_drugs_for_side_effect",
    "is_side_effect_for_drug",
    "get_ontology_child_terms",
    "get_ontology_parent_terms",
    "isa_or_partof",
    "get_pmids_for_mesh",
    "get_mesh_ids_for_pmid",
    "get_evidences_for_mesh",
    "get_evidences_for_stmt_hash",
    "get_evidences_for_stmt_hashes",
    "get_stmts_for_pmid",
    "get_stmts_for_mesh",
    "get_stmts_for_stmt_hashes",
    "is_gene_mutated",
    "get_drugs_for_target",
    "get_targets_for_drug",
    "is_drug_target",
    # Summary functions
    "get_node_counter",
    "get_edge_counter",
    "get_schema_graph",
]


# BGee


@autoclient()
def get_genes_in_tissue(
    tissue: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return the genes in the given tissue.

    Parameters
    ----------
    client :
        The Neo4j client.
    tissue :
        The tissue to query.

    Returns
    -------
    :
        The genes expressed in the given tissue.
    """
    return client.get_sources(
        tissue,
        relation="expressed_in",
        source_type="BioEntity",
        target_type="BioEntity",
    )


@autoclient()
def get_tissues_for_gene(
    gene: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return the tissues the gene is expressed in.

    Parameters
    ----------
    client :
        The Neo4j client.
    gene :
        The gene to query.

    Returns
    -------
    :
        The tissues the gene is expressed in.
    """
    return client.get_targets(
        gene,
        relation="expressed_in",
        source_type="BioEntity",
        target_type="BioEntity",
    )


@autoclient()
def is_gene_in_tissue(
    gene: Tuple[str, str], tissue: Tuple[str, str], *, client: Neo4jClient
) -> bool:
    """Return True if the gene is expressed in the given tissue.

    Parameters
    ----------
    client :
        The Neo4j client.
    gene :
        The gene to query.
    tissue :
        The tissue to query.

    Returns
    -------
    :
        True if the gene is expressed in the given tissue.
    """
    return client.has_relation(
        gene,
        tissue,
        relation="expressed_in",
        source_type="BioEntity",
        target_type="BioEntity",
    )


# GO


@autoclient()
def get_go_terms_for_gene(
    gene: Tuple[str, str], include_indirect=False, *, client: Neo4jClient
) -> Iterable[Node]:
    """Return the GO terms for the given gene.

    Parameters
    ----------
    client :
        The Neo4j client.
    gene :
        The gene to query.

    Returns
    -------
    :
        The GO terms for the given gene.
    """
    go_term_nodes = client.get_targets(gene, relation="associated_with")
    if not include_indirect:
        return go_term_nodes
    go_terms = {gtn.grounding(): gtn for gtn in go_term_nodes}
    for go_term_node in go_term_nodes:
        go_child_terms = client.get_successors(
            go_term_node.grounding(),
            relations=["isa"],
            source_type="BioEntity",
            target_type="BioEntity",
        )
        for term in go_child_terms:
            go_terms[term.grounding()] = term
    return list(go_terms.values())


@autoclient()
def get_genes_for_go_term(
    go_term: Tuple[str, str], include_indirect: bool = False, *, client: Neo4jClient
) -> Iterable[Node]:
    """Return the genes associated with the given GO term.

    Parameters
    ----------
    client :
        The Neo4j client.
    go_term :
        The GO term to query.
    include_indirect :
        Should ontological children of the given GO term
        be queried as well? Defaults to False.

    Returns
    -------
    :
        The genes associated with the given GO term.
    """
    go_children = get_ontology_child_terms(client, go_term) if include_indirect else []
    gene_nodes = {}
    for term in [go_term] + go_children:
        genes = client.get_sources(
            term,
            relation="associated_with",
            source_type="BioEntity",
            target_type="BioEntity",
        )
        for gene in genes:
            gene_nodes[gene.grounding()] = gene
    return list(gene_nodes.values())


@autoclient()
def is_go_term_for_gene(
    gene: Tuple[str, str], go_term: Tuple[str, str], *, client: Neo4jClient
) -> bool:
    """Return True if the given GO term is associated with the given gene.

    Parameters
    ----------
    client :
        The Neo4j client.
    gene :
        The gene to query.
    go_term :
        The GO term to query.

    Returns
    -------
    :
        True if the given GO term is associated with the given gene.
    """
    return client.has_relation(
        gene,
        go_term,
        relation="associated_with",
        source_type="BioEntity",
        target_type="BioEntity",
    )


# Trials


@autoclient()
def get_trials_for_drug(
    drug: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return the trials for the given drug.

    Parameters
    ----------
    client :
        The Neo4j client.
    drug :
        The drug to query.

    Returns
    -------
    :
        The trials for the given drug.
    """
    return client.get_targets(
        drug,
        relation="tested_in",
        source_type="BioEntity",
        target_type="ClinicalTrial",
    )


@autoclient()
def get_trials_for_disease(
    disease: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return the trials for the given disease.

    Parameters
    ----------
    client :
        The Neo4j client.
    disease :
        The disease to query.

    Returns
    -------
    :
        The trials for the given disease.
    """
    return client.get_targets(
        disease,
        relation="has_trial",
        source_type="BioEntity",
        target_type="ClinicalTrial",
    )


@autoclient()
def get_drugs_for_trial(
    trial: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return the drugs for the given trial.

    Parameters
    ----------
    client :
        The Neo4j client.
    trial :
        The trial to query.

    Returns
    -------
    :
        The drugs for the given trial.
    """
    return client.get_sources(
        trial,
        relation="tested_in",
        source_type="BioEntity",
        target_type="ClinicalTrial",
    )


@autoclient()
def get_diseases_for_trial(
    trial: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return the diseases for the given trial.

    Parameters
    ----------
    client :
        The Neo4j client.
    trial :
        The trial to query.

    Returns
    -------
    :
        The diseases for the given trial.
    """
    return client.get_sources(
        trial,
        relation="has_trial",
        source_type="BioEntity",
        target_type="ClinicalTrial",
    )


# Pathways


@autoclient()
def get_pathways_for_gene(
    gene: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return the pathways for the given gene.

    Parameters
    ----------
    client :
        The Neo4j client.
    gene :
        The gene to query.

    Returns
    -------
    :
        The pathways for the given gene.
    """
    return client.get_sources(
        gene,
        relation="haspart",
        source_type="BioEntity",
        target_type="BioEntity",
    )


@autoclient()
def get_shared_pathways_for_genes(
    client: Neo4jClient, genes: Iterable[Tuple[str, str]]
) -> Iterable[Node]:
    """Return the shared pathways for the given list of genes.

    Parameters
    ----------
    client :
        The Neo4j client.
    genes :
        The list of genes to query.

    Returns
    -------
    :
        The pathways for the given gene.
    """
    return client.get_common_sources(
        genes,
        relation="haspart",
        source_type="BioEntity",
        target_type="BioEntity",
    )


def get_genes_for_pathway(
    pathway: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return the genes for the given pathway.

    Parameters
    ----------
    client :
        The Neo4j client.
    pathway :
        The pathway to query.

    Returns
    -------
    :
        The genes for the given pathway.
    """
    return client.get_targets(
        pathway,
        relation="haspart",
        source_type="BioEntity",
        target_type="BioEntity",
    )


@autoclient()
def is_gene_in_pathway(
    gene: Tuple[str, str], pathway: Tuple[str, str], *, client: Neo4jClient
) -> bool:
    """Return True if the gene is in the given pathway.

    Parameters
    ----------
    client :
        The Neo4j client.
    gene :
        The gene to query.
    pathway :
        The pathway to query.

    Returns
    -------
    :
        True if the gene is in the given pathway.
    """
    return client.has_relation(
        pathway,
        gene,
        relation="haspart",
        source_type="BioEntity",
        target_type="BioEntity",
    )


# Side effects


@autoclient()
def get_side_effects_for_drug(
    drug: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return the side effects for the given drug.

    Parameters
    ----------
    client :
        The Neo4j client.
    drug :
        The drug to query.

    Returns
    -------
    :
        The side effects for the given drug.
    """
    return client.get_targets(
        drug,
        relation="has_side_effect",
        source_type="BioEntity",
        target_type="BioEntity",
    )


@autoclient()
def get_drugs_for_side_effect(
    side_effect: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return the drugs for the given side effect.

    Parameters
    ----------
    client :
        The Neo4j client.
    side_effect :
        The side effect to query.

    Returns
    -------
    :
        The drugs for the given side effect.
    """
    return client.get_sources(
        side_effect,
        relation="has_side_effect",
        source_type="BioEntity",
        target_type="BioEntity",
    )


@autoclient()
def is_side_effect_for_drug(
    drug: Tuple[str, str], side_effect: Tuple[str, str], *, client: Neo4jClient
) -> bool:
    """Return True if the given side effect is associated with the given drug.

    Parameters
    ----------
    client :
        The Neo4j client.
    drug :
        The drug to query.
    side_effect :
        The side effect to query.

    Returns
    -------
    :
        True if the given side effect is associated with the given drug.
    """
    return client.has_relation(
        drug,
        side_effect,
        relation="has_side_effect",
        source_type="BioEntity",
        target_type="BioEntity",
    )


# Ontology


@autoclient()
def get_ontology_child_terms(
    term: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return the child terms of the given term.

    Parameters
    ----------
    client :
        The Neo4j client.
    term :
        The term to query.

    Returns
    -------
    :
        The child terms of the given term.
    """
    return client.get_predecessors(term, relations={"isa", "partof"})


@autoclient()
def get_ontology_parent_terms(
    term: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return the parent terms of the given term.

    Parameters
    ----------
    client :
        The Neo4j client.
    term :
        The term to query.

    Returns
    -------
    :
        The parent terms of the given term.
    """
    return client.get_successors(term, relations={"isa", "partof"})


@autoclient()
def isa_or_partof(
    term: Tuple[str, str], parent: Tuple[str, str], *, client: Neo4jClient
) -> bool:
    """Return True if the given term is a child of the given parent.

    Parameters
    ----------
    client :
        The Neo4j client.
    term :
        The term to query.
    parent :
        The parent to query.

    Returns
    -------
    :
        True if the given term is a child term of the given parent.
    """
    term_parents = get_ontology_parent_terms(term, client=client)
    return any(parent == parent_term.grounding() for parent_term in term_parents)


# MESH / PMID


@autoclient()
def get_pmids_for_mesh(
    mesh_term: Tuple[str, str], include_child_terms: bool = True, *, client: Neo4jClient
) -> Iterable[Node]:
    """Return the PubMed IDs for the given MESH term.

    Parameters
    ----------
    client :
        The Neo4j client.
    mesh_term :
        The MESH term to query.
    include_child_terms :
        If True, also match against the child MESH terms of the given MESH
        term.

    Returns
    -------
    :
        The PubMed IDs for the given MESH term and, optionally, its child terms.
    """
    if mesh_term[0] != "MESH":
        raise ValueError(f"Expected MESH term, got %s" % str(mesh_term))
    norm_mesh = norm_id(*mesh_term)

    # NOTE: we could use get_ontology_child_terms() here, but it's ~20 times
    # slower for this specific query, so we do an optimized query that is
    # basically equivalent instead.
    if include_child_terms:
        child_terms = _get_mesh_child_terms(mesh_term, client=client)
    else:
        child_terms = set()

    if child_terms:
        terms = {norm_mesh} | child_terms
        terms_str = ",".join(f'"{c}"' for c in terms)
        query = (
            """MATCH (k:Publication)-[:annotated_with]->(b:BioEntity)
               WHERE b.id IN [%s]
               RETURN DISTINCT k"""
            % terms_str
        )
    else:
        match_clause = (
            'MATCH (k:Publication)-[:annotated_with]->(b:BioEntity {id: "%s"})'
            % norm_mesh
        )
        query = "%s RETURN k" % match_clause

    return [client.neo4j_to_node(r[0]) for r in client.query_tx(query)]


@autoclient()
def get_mesh_ids_for_pmid(
    pmid_term: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return the MESH terms for the given PubMed ID.

    Parameters
    ----------
    client :
        The Neo4j client.
    pmid_term :
        The PubMed ID to query.

    Returns
    -------
    :
        The MESH terms for the given PubMed ID.
    """
    if pmid_term[0] != "PUBMED":
        raise ValueError(f"Expected PUBMED term, got %s" % str(pmid_term))

    return client.get_targets(
        source=pmid_term,
        relation="annotated_with",
        source_type="Publication",
        target_type="BioEntity",
    )


@autoclient()
def get_evidences_for_mesh(
    mesh_term: Tuple[str, str], include_child_terms: bool = True, *, client: Neo4jClient
) -> Dict[str, List[Evidence]]:
    """Return the evidence objects for the given MESH term.

    Parameters
    ----------
    client :
        The Neo4j client.
    mesh_term :
        The MESH ID to query.
    include_child_terms :
        If True, also match against the child MESH terms of the given MESH ID

    Returns
    -------
    :
        The evidence objects for the given MESH ID grouped into a dict
        by statement hash.
    """
    if mesh_term[0] != "MESH":
        raise ValueError(f"Expected MESH term, got %s" % str(mesh_term))

    norm_mesh = norm_id(*mesh_term)
    if include_child_terms:
        child_terms = _get_mesh_child_terms(mesh_term, client=client)
    else:
        child_terms = set()

    if child_terms:
        terms = {norm_mesh} | child_terms
        terms_str = ",".join(f'"{c}"' for c in terms)
        where_clause = "WHERE b.id IN [%s]" % terms_str
        single_mesh_match = ""
    else:
        single_mesh_match = ' {id: "%s"}' % norm_mesh
        where_clause = ""

    query = """MATCH (e:Evidence)-[:has_citation]->(:Publication)-[:annotated_with]->(b:BioEntity%s)
           %s
           RETURN e.stmt_hash, e.evidence""" % (
        single_mesh_match,
        where_clause,
    )
    return _get_ev_dict_from_hash_ev_query(client.query_tx(query))


@autoclient()
def get_evidences_for_stmt_hash(
    stmt_hash: Union[str, int], *, client: Neo4jClient
) -> Iterable[Evidence]:
    """Return the matching evidence objects for the given statement hash.

    Parameters
    ----------
    client :
        The Neo4j client.
    stmt_hash :
        The statement hash to query, accepts both string and integer.

    Returns
    -------
    :
        The evidence objects for the given statement hash.
    """
    query = (
        """MATCH (n:Evidence {stmt_hash: "%s"})
               RETURN n.evidence"""
        % stmt_hash
    )
    ev_jsons = [json.loads(r[0]) for r in client.query_tx(query)]
    return [Evidence._from_json(ev_json) for ev_json in ev_jsons]


@autoclient()
def get_evidences_for_stmt_hashes(
    stmt_hashes: Iterable[Union[str, int]], *, client: Neo4jClient
) -> Dict[str, List[Evidence]]:
    """Return the matching evidence objects for the given statement hashes.

    Parameters
    ----------
    client :
        The Neo4j client.
    stmt_hashes :
        The statement hashes to query, accepts integers and strings.

    Returns
    -------
    :
        A mapping of stmt hash to a list of evidence objects for the given
        statement hashes.
    """
    stmt_hashes_str = ",".join(f'"{h}"' for h in stmt_hashes)
    query = (
        """
        MATCH (n:Evidence)
        WHERE n.stmt_hash IN [%s]
        RETURN n.stmt_hash, n.evidence
    """
        % stmt_hashes_str
    )

    return _get_ev_dict_from_hash_ev_query(client.query_tx(query))


@autoclient()
def get_stmts_for_pmid(
    pmid_term: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Statement]:
    """Return the statements with evidence from the given PubMed ID.

    Parameters
    ----------
    client :
        The Neo4j client.
    pmid_term :
        The PubMed ID to query.

    Returns
    -------
    :
        The statements for the given PubMed ID.
    """
    # Todo: Investigate if it's possible to do this in one query like
    # MATCH (e:Evidence)-[:has_citation]->(:Publication {id: "pubmed:14898026"})
    # MATCH (:BioEntity)-[r:indra_rel {stmt_hash: toInteger(e.stmt_hash)}]->(:BioEntity)
    # RETURN r, e
    # note that the hash str/int conversion is done as part of the query.

    # Todo: Add filters: e.g. belief cutoff, sources, db supported only,
    #  stmt type
    pmid_norm = norm_id(*pmid_term)
    # First, get the hashes and evidences for the given PubMed ID
    hash_query = (
        """
        MATCH (e:Evidence)-[:has_citation]->(:Publication {id: "%s"})
        RETURN e.stmt_hash, e.evidence
    """
        % pmid_norm
    )
    result = client.query_tx(hash_query)
    ev_dict = _get_ev_dict_from_hash_ev_query(result)
    stmt_hashes = set(ev_dict.keys())
    return get_stmts_for_stmt_hashes(stmt_hashes, ev_dict, client=client)


@autoclient()
def get_stmts_for_mesh(
    mesh_term: Tuple[str, str], include_child_terms: bool = True, *, client: Neo4jClient
) -> Iterable[Statement]:
    """Return the statements with evidence for the given MESH ID.

    Parameters
    ----------
    client :
        The Neo4j client.
    mesh_term :
        The MESH ID to query.
    include_child_terms :
        If True, also match against the children of the given MESH ID.

    Returns
    -------
    :
        The statements for the given MESH ID.
    """
    ev_dict = get_evidences_for_mesh(mesh_term, include_child_terms, client=client)
    hashes = list(ev_dict.keys())
    return get_stmts_for_stmt_hashes(hashes, ev_dict, client=client)


@autoclient()
def get_stmts_for_stmt_hashes(
    stmt_hashes: Iterable[str],
    evidence_map: Optional[Dict[str, List[Evidence]]] = None,
    *,
    client: Neo4jClient,
) -> Iterable[Statement]:
    """Return the statements for the given statement hashes.

    Parameters
    ----------
    client :
        The Neo4j client.
    stmt_hashes :
        The statement hashes to query.
    evidence_map :
        Optionally provide a mapping of stmt hash to a list of evidence objects

    Returns
    -------
    :
        The statements for the given statement hashes.
    """
    stmt_hashes_str = ",".join(stmt_hashes)
    # NOTE: for indra_rel Relationships, stmt_hash is an int, in Evidence
    # nodes stmt_hash is a string
    stmts_query = (
        """
        MATCH p=(:BioEntity)-[r:indra_rel]->(:BioEntity)
        WHERE r.stmt_hash IN [%s]
        RETURN p
    """
        % stmt_hashes_str
    )
    rels = [client.neo4j_to_relation(r[0]) for r in client.query_tx(stmts_query)]
    stmts = {str(s.get_hash()): s for s in indra_stmts_from_relations(rels)}

    # If the evidence_map is provided, check if it covers all the hashes
    # and if not, query for the evidence objects
    evidence_map = evidence_map or {}
    if evidence_map:
        missing_hashes = list(set(stmt_hashes) - set(evidence_map.keys()))
    else:
        missing_hashes = stmt_hashes

    # Get the evidence objects for the given statement hashes
    if missing_hashes:
        new_evidences = get_evidences_for_stmt_hashes(stmt_hashes, client=client)
        if evidence_map:
            evidence_map.update(new_evidences)
        else:
            evidence_map = new_evidences

    # Add the evidence objects to the statements, if no result, keep the
    # original statement evidence
    for stmt_hash, stmt in stmts.items():
        ev_list = evidence_map.get(stmt_hash, [])
        if ev_list:
            stmt.evidence = ev_list
        else:
            logger.warning(
                f"No evidence for stmt hash {stmt_hash}, keeping sample evidence"
            )

    return list(stmts.values())


@autoclient()
def _get_mesh_child_terms(
    mesh_term: Tuple[str, str], *, client: Neo4jClient
) -> Set[str]:
    """Return the children of the given MESH ID.

    Parameters
    ----------
    client :
        The Neo4j client.
    mesh_term :
        The MESH ID to query.

    Returns
    -------
    :
        The children of the given MESH ID using the ID standard internal
        to the graph.
    """
    meshid_norm = norm_id(*mesh_term)
    query = (
        """
        MATCH (c:BioEntity)-[:isa*1..]->(:BioEntity {id: "%s"})
        RETURN DISTINCT c.id
    """
        % meshid_norm
    )
    return {c[0] for c in client.query_tx(query)}


def _get_ev_dict_from_hash_ev_query(
    result: Optional[Iterable[List[str]]] = None,
) -> Dict[str, List[Evidence]]:
    """Assumes `result` is an Iterable of pairs of [hash, evidence_json]"""
    if result is None:
        logger.warning("No result for hash, Evidence query, returning empty dict")
        return {}

    ev_dict = defaultdict(list)
    for stmt_hash, ev_json_str in result:
        ev_json = json.loads(ev_json_str)
        ev_dict[stmt_hash].append(Evidence._from_json(ev_json))
    return dict(ev_dict)


@autoclient(cache=True)
def get_node_counter(*, client: Neo4jClient) -> Counter:
    """Get a count of each entity type.

    .. warning:: this code assumes all nodes only have one label, as in``label[0]``
    """
    return Counter(
        {
            label[0]: client.query_tx(f"MATCH (n:{label[0]}) RETURN count(*)")[0][0]
            for label in client.query_tx("call db.labels();")
        }
    )


@autoclient(cache=True)
def get_edge_counter(*, client: Neo4jClient) -> Counter:
    """Get a count of each edge type."""
    return Counter(
        {
            relation[0]: client.query_tx(
                f"MATCH ()-[r:{relation[0]}]->() RETURN count(*)"
            )[0][0]
            for relation in client.query_tx("call db.relationshipTypes();")
        }
    )


@autoclient(cache=True)
def get_schema_graph(*, client: Neo4jClient) -> nx.MultiDiGraph:
    """Get a NetworkX graph reflecting the schema of the Neo4j graph.

    Generate a PDF diagram (works with PNG and SVG too) with the following::

    >>> from networkx.drawing.nx_agraph import to_agraph
    >>> client = ...
    >>> graph = get_schema_graph(client)
    >>> to_agraph(graph).draw("~/Desktop/cogex_schema.pdf", prog="dot")
    """
    query = "call db.schema.visualization();"
    schema_nodes, schema_relationships = client.query_tx(query)[0]

    graph = nx.MultiDiGraph()
    for node in schema_nodes:
        graph.add_node(node._id, label=node["name"])
    for edge in schema_relationships:
        graph.add_edge(
            edge.start_node._id,
            edge.end_node._id,
            id=edge._id,
            label=edge.type,
        )
    return graph


# CCLE


def is_gene_mutated(
    client: Neo4jClient, gene: Tuple[str, str], cell_line: Tuple[str, str]
) -> bool:
    """Return True if the gene is mutated in the given cell line.

    Parameters
    ----------
    client :
        The Neo4j client.
    gene :
        The gene to query.
    cell_line :
        The cell line to query.

    Returns
    -------
    :
        True if the gene is mutated in the given cell line.
    """
    return client.has_relation(
        gene,
        cell_line,
        relation="mutated_in",
        source_type="BioEntity",
        target_type="BioEntity",
    )


# Indra DB


def get_drugs_for_target(
    client: Neo4jClient, target: Tuple[str, str]
) -> Iterable[Agent]:
    """Return the drugs targetting the given protein."""
    rels = client.get_source_relations(
        target, "indra_rel", source_type="BioEntity", target_type="BioEntity"
    )
    drug_rels = [rel for rel in rels if _is_drug_relation(rel)]
    drug_nodes = [(rel.source_ns, rel.source_id) for rel in drug_rels]
    stmt_jsons = [json.loads(rel.data["stmt_json"]) for rel in drug_rels]
    stmts = stmts_from_json(stmt_jsons)
    drug_agents = [stmt.subj for stmt in stmts]
    return {"nodes": drug_nodes, "agents": drug_agents}


def get_targets_for_drug(client: Neo4jClient, drug: Tuple[str, str]) -> Iterable[Agent]:
    """Return the proteins targetted by the given drug."""
    rels = client.get_target_relations(
        drug, "indra_rel", source_type="BioEntity", target_type="BioEntity"
    )
    target_rels = [rel for rel in rels if _is_drug_relation(rel)]
    target_nodes = [(rel.target_ns, rel.target_id) for rel in target_rels]
    stmt_jsons = [json.loads(rel.data["stmt_json"]) for rel in target_rels]
    stmts = stmts_from_json(stmt_jsons)
    target_agents = [stmt.obj for stmt in stmts]
    return {"nodes": target_nodes, "agents": target_agents}


def is_drug_target(
    client: Neo4jClient, drug: Tuple[str, str], target: Tuple[str, str]
) -> bool:
    """Return True if the drug targets the given protein."""
    rels = client.get_relations(
        drug, target, "indra_rel", source_type="BioEntity", target_type="BioEntity"
    )
    return any(_is_drug_relation(rel) for rel in rels)


def _is_drug_relation(rel: Relation) -> bool:
    """Return True if the relation is a drug-target relation."""
    return rel.data["stmt_type"] == "Inhibition" and "tas" in rel.data["source_counts"]
