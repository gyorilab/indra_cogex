import json
import logging
import time
from collections import Counter, defaultdict
from textwrap import dedent
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple, Union

import networkx as nx
from indra.statements import Agent, Evidence, Statement

from .neo4j_client import Neo4jClient, autoclient
from ..representation import Node, Relation, indra_stmts_from_relations, norm_id

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
    "get_mesh_ids_for_pmids",
    "get_evidences_for_mesh",
    "get_evidences_for_stmt_hash",
    "get_evidences_for_stmt_hashes",
    "get_stmts_for_paper",
    "get_stmts_for_pmids",
    "get_stmts_for_mesh",
    "get_stmts_meta_for_stmt_hashes",
    "get_stmts_for_stmt_hashes",
    "get_statements_mix",
    "get_stmts_for_agent_type",
    "get_stmts_for_source",
    "get_stmts_for_rel_type",
    "is_gene_mutated",
    "get_mutated_genes",
    "get_drugs_for_target",
    "get_drugs_for_targets",
    "get_targets_for_drug",
    "get_targets_for_drugs",
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
    gene: Tuple[str, str], include_indirect: bool = False, *, client: Neo4jClient
) -> Iterable[Node]:
    """Return the GO terms for the given gene.

    Parameters
    ----------
    client :
        The Neo4j client.
    gene :
        The gene to query.
    include_indirect :
        If True, also return indirect GO terms.

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
        The GO term to query. Example: ``("GO", "GO:0006915")``
    include_indirect :
        Should ontological children of the given GO term
        be queried as well? Defaults to False.

    Returns
    -------
    :
        The genes associated with the given GO term.
    """
    go_children = (
        get_ontology_child_terms(go_term, client=client) if include_indirect else []
    )
    gene_nodes = {}
    for term in [go_term] + [gc.grounding() for gc in go_children]:
        genes = client.get_sources(
            term,
            relation="associated_with",
            source_type="BioEntity",
            target_type="BioEntity",
        )
        for gene in genes:
            gene_grnd = gene.grounding()
            if gene_grnd[0] == "HGNC":
                gene_nodes[gene_grnd] = gene
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
    genes: Iterable[Tuple[str, str]], *, client: Neo4jClient
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


@autoclient()
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
    return client.get_predecessors(
        term,
        relations={"isa", "partof"},
        source_type="BioEntity",
        target_type="BioEntity",
    )


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
    return client.get_successors(
        term,
        relations={"isa", "partof"},
        source_type="BioEntity",
        target_type="BioEntity",
    )


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
        raise ValueError("Expected MESH term, got %s" % str(mesh_term))
    norm_mesh = norm_id(*mesh_term)

    # NOTE: we could use get_ontology_child_terms() here, but it's ~20 times
    # slower for this specific query, so we do an optimized query that is
    # basically equivalent instead.
    if include_child_terms:
        child_terms = _get_mesh_child_terms(mesh_term, client=client)
    else:
        child_terms = set()

    query_param = {}
    if child_terms:
        mesh_terms = {norm_mesh} | child_terms
        query = (
            """MATCH (k:Publication)-[:annotated_with]->(b:BioEntity)
               WHERE b.id IN $mesh_terms
               RETURN DISTINCT k"""
        )
        query_param["mesh_terms"] = mesh_terms
    else:
        query = (
            'MATCH (k:Publication)-[:annotated_with]->'
            '(b:BioEntity {id: $mesh_term}) RETURN k'
        )
        query_param["mesh_term"] = norm_mesh

    return client.query_nodes(query, **query_param)


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
        raise ValueError("Expected PUBMED term, got %s" % str(pmid_term))

    return client.get_targets(
        source=pmid_term,
        relation="annotated_with",
        source_type="Publication",
        target_type="BioEntity",
    )


@autoclient()
def get_mesh_ids_for_pmids(
    pmids: List[str], *, client: Neo4jClient
) -> Mapping[str, List[str]]:
    """Return the MESH terms for the given PubMed ID.

    Parameters
    ----------
    client :
        The Neo4j client.
    pmids :
        The PubMed IDs to query.

    Returns
    -------
    :
        A dictionary from PubMed ID to MeSH IDs
    """
    pmid_terms = [("PUBMED", pubmed_id) for pubmed_id in pmids]
    res = client.get_target_relations_for_sources(
        sources=pmid_terms,
        relation="annotated_with",
        source_type="Publication",
        target_type="BioEntity",
    )
    return {
        pubmed: [r.target_id for r in relations]
        for (_, pubmed), relations in res.items()
    }


@autoclient()
def get_evidences_for_mesh(
    mesh_term: Tuple[str, str], include_child_terms: bool = True, *, client: Neo4jClient
) -> Dict[int, List[Evidence]]:
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
        raise ValueError("Expected MESH term, got %s" % str(mesh_term))

    norm_mesh = norm_id(*mesh_term)
    if include_child_terms:
        child_terms = _get_mesh_child_terms(mesh_term, client=client)
    else:
        child_terms = set()

    query_params = {}
    if child_terms:
        match_terms = {norm_mesh} | child_terms
        where_clause = "WHERE b.id IN $mesh_terms"
        single_mesh_match = ""
        query_params["mesh_terms"] = match_terms
    else:
        single_mesh_match = ' {id: $mesh_id}'
        where_clause = ""
        query_params["mesh_id"] = norm_mesh

    query = """MATCH (e:Evidence)-[:has_citation]->(:Publication)-[:annotated_with]->(b:BioEntity%s)
           %s
           RETURN e.stmt_hash, e.evidence""" % (
        single_mesh_match,
        where_clause,
    )
    return _get_ev_dict_from_hash_ev_query(
        client.query_tx(query, **query_params), remove_medscan=True
    )


@autoclient()
def get_evidences_for_stmt_hash(
    stmt_hash: int,
    *,
    client: Neo4jClient,
    limit: Optional[int] = None,
    offset: int = 0,
    remove_medscan: bool = True,
) -> Iterable[Evidence]:
    """Return the matching evidence objects for the given statement hash.

    Parameters
    ----------
    client :
        The Neo4j client.
    stmt_hash :
        The statement hash to query, accepts both string and integer.
    limit :
        The maximum number of results to return.
    offset :
        The number of results to skip before returning the first result.
    remove_medscan :
        If True, remove the MedScan evidence from the results.

    Returns
    -------
    :
        The evidence objects for the given statement hash.
    """
    remove_medscan = True  # Always remove medscan for now
    query_params = {"stmt_hash": stmt_hash}
    if remove_medscan:
        where_clause = "WHERE n.source_api <> $source_api\n"
        query_params["source_api"] = "medscan"
    else:
        where_clause = ""
    query = (
        """MATCH (n:Evidence {stmt_hash: $stmt_hash})
               %sRETURN n.evidence"""
        % where_clause
    )

    # Add limit and offset
    if offset > 0 or limit is not None:
        # Order by the node internal ID to ensure that results are returned
        # in a persistent order. Do NOT expose the internal ID to the
        # user as the id is not guaranteed to be persistent when nodes are
        # added or removed.
        query += "\nORDER BY id(n)"

    if offset > 0:
        query += "\nSKIP %d" % offset
    if limit is not None and limit > 0:
        query += "\nLIMIT %d" % limit
    ev_jsons = [json.loads(r) for r in
                client.query_tx(query, squeeze=True, **query_params)]
    return _filter_out_medscan_evidence(ev_list=ev_jsons, remove_medscan=True)


@autoclient()
def get_evidences_for_stmt_hashes(
    stmt_hashes: Iterable[int],
    *,
    client: Neo4jClient,
    limit: Optional[str] = None,
    remove_medscan: bool = True,
) -> Dict[int, List[Evidence]]:
    """Return the matching evidence objects for the given statement hashes.

    Parameters
    ----------
    client :
        The Neo4j client.
    stmt_hashes :
        The statement hashes to query, accepts integers and strings.
    limit:
        The optional maximum number of evidences returned for each statement hash
    remove_medscan :
        If True, remove the MedScan evidence from the results.

    Returns
    -------
    :
        A mapping of stmt hash to a list of evidence objects for the given
        statement hashes.
    """
    limit_box = "" if limit is None else f"[..{limit}]"
    query = f"""\
        MATCH (n:Evidence)
        WHERE
            n.stmt_hash IN $stmt_hashes
            AND n.source_api <> $source_api
        RETURN n.stmt_hash, collect(n.evidence){limit_box}
    """
    result = client.query_tx(
        query, stmt_hashes=stmt_hashes, source_api="medscan"
    )
    return {
        stmt_hash: _filter_out_medscan_evidence(
            (json.loads(evidence_str) for evidence_str in evidences),
            remove_medscan=remove_medscan,
        )
        for stmt_hash, evidences in result
    }


@autoclient()
def get_stmts_for_paper(
    paper_term: Tuple[str, str], *, client: Neo4jClient, **kwargs
) -> List[Statement]:
    """Return the statements with evidence from the given PubMed ID.

    Parameters
    ----------
    client :
        The Neo4j client.
    paper_term :
        The term to query. Can be a PubMed ID, PMC id, TRID, or DOI

    Returns
    -------
    :
        The statements for the given PubMed ID.
    """
    # Todo: Investigate if it's possible to do this in one query like
    # MATCH (e:Evidence)-[:has_citation]->(:Publication {id: "pubmed:14898026"})
    # MATCH (:BioEntity)-[r:indra_rel {stmt_hash: e.stmt_hash}]->(:BioEntity)
    # RETURN r, e

    # Todo: Add filters: e.g. belief cutoff, sources, db supported only,
    #  stmt type

    if paper_term[0].lower() in {"pmid", "pubmed"}:
        parameter = norm_id(*paper_term)
        publication_props = "{id: $parameter}"

    elif paper_term[0].lower() == "doi":
        parameter = paper_term[1]
        publication_props = "{doi: $parameter}"

    elif paper_term[0].lower() in {"pmc", "pmcid"}:
        parameter = paper_term[1]
        publication_props = "{pmcid: $parameter}"

    elif paper_term[0].lower() == "trid":
        parameter = paper_term[1]
        publication_props = "{trid: $parameter}"

    else:
        raise ValueError(f"Invalid prefix for publication lookup: {paper_term[0]}")

    hash_query = f"""\
        MATCH (e:Evidence)-[:has_citation]->(:Publication {publication_props})
        RETURN e.stmt_hash, e.evidence
    """
    result = client.query_tx(hash_query, parameter=parameter)
    return _stmts_from_results(client=client, result=result, **kwargs)


@autoclient()
def get_stmts_for_pmids(
    pmids: List[Union[str, int]], *, client: Neo4jClient, **kwargs
) -> List[Statement]:
    """Return the statements with evidence from the given PubMed IDs.

    Parameters
    ----------
    client :
        The Neo4j client.
    pmids :
        The PMIDs to query

    Returns
    -------
    :
        The statements for the given PubMed identifiers.

    Example
    -------
    .. code-block::

        from indra_cogex.client.queries import get_stmts_for_pmids

        pmids = [20861832, 19503834]
        stmts = get_stmts_for_pmids(pmids)
    """
    pmids = sorted(f"pubmed:{pmid}" for pmid in pmids)
    hash_query = f"""\
        MATCH (e:Evidence)-[:has_citation]->(p:Publication)
        WHERE p.id IN {repr(pmids)}
        RETURN e.stmt_hash, e.evidence
    """
    result = client.query_tx(hash_query)
    return _stmts_from_results(client=client, result=result, **kwargs)


def _stmts_from_results(client, result, **kwargs) -> List[Statement]:
    evidence_map = _get_ev_dict_from_hash_ev_query(result, remove_medscan=True)
    stmt_hashes = set(evidence_map.keys())
    return get_stmts_for_stmt_hashes(
        stmt_hashes, evidence_map=evidence_map, client=client, **kwargs
    )


@autoclient()
def get_stmts_for_mesh(
    mesh_term: Tuple[str, str],
    include_child_terms: bool = True,
    *,
    client: Neo4jClient,
    **kwargs,
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
    kwargs:
        Additional keyword arguments to forward to
        :func:`get_stmts_for_stmt_hashes`

    Returns
    -------
    :
        The statements for the given MESH ID.
    """
    evidence_map = get_evidences_for_mesh(mesh_term, include_child_terms, client=client)
    hashes = list(evidence_map.keys())
    return get_stmts_for_stmt_hashes(
        hashes,
        evidence_map=evidence_map,
        client=client,
        **kwargs,
    )


@autoclient()
def get_stmts_meta_for_stmt_hashes(
    stmt_hashes: Iterable[int],
    *,
    client: Neo4jClient,
) -> Iterable[Relation]:
    """Return the metadata and statements for a given list of hashes

    Parameters
    ----------
    stmt_hashes :
        The list of statement hashes to query.
    client :
        The Neo4j client.

    Returns
    -------
    :
        A dict of statements with their metadata
    """
    stmt_hashes_str = ",".join(str(h) for h in stmt_hashes)
    query = dedent(
        f"""
        MATCH p=(:BioEntity)-[r:indra_rel]->(:BioEntity)
        WHERE r.stmt_hash IN [{stmt_hashes_str}]
        RETURN p"""
    )
    result = client.query_tx(query)
    return [client.neo4j_to_relation(r[0]) for r in result]


@autoclient()
def get_stmts_for_stmt_hashes(
    stmt_hashes: Iterable[int],
    *,
    evidence_map: Optional[Dict[int, List[Evidence]]] = None,
    client: Neo4jClient,
    evidence_limit: Optional[int] = None,
    return_evidence_counts: bool = False,
    subject_prefix: Optional[str] = None,
    object_prefix: Optional[str] = None,
) -> Union[List[Statement], Tuple[List[Statement], Mapping[int, int]]]:
    """Return the statements for the given statement hashes.

    Parameters
    ----------
    client :
        The Neo4j client.
    stmt_hashes :
        The statement hashes to query.
    evidence_map :
        Optionally provide a mapping of stmt hash to a list of evidence objects
    evidence_limit:
        An optional maximum number of evidences to return

    Returns
    -------
    :
        The statements for the given statement hashes.
    """
    query_params = {"stmt_hashes": list(stmt_hashes)}
    if subject_prefix:
        subject_constraint = f"AND a.id STARTS WITH $subject_prefix"
        query_params["subject_prefix"] = subject_prefix
    else:
        subject_constraint = ""

    if object_prefix:
        object_constraint = f"AND b.id STARTS WITH $object_prefix"
        query_params["object_prefix"] = object_prefix
    else:
        object_constraint = ""

    stmts_query = f"""\
        MATCH p=(a:BioEntity)-[r:indra_rel]->(b:BioEntity)
        WHERE
            r.stmt_hash IN $stmt_hashes
            {subject_constraint}
            {object_constraint}
        RETURN p
    """
    logger.info(f"Getting statements for {len(stmt_hashes)} hashes")
    rels = client.query_relations(stmts_query, **query_params)
    stmts = indra_stmts_from_relations(rels, deduplicate=True)

    if evidence_limit == 1:
        rv = stmts
    else:
        rv = enrich_statements(
            stmts,
            client=client,
            evidence_map=evidence_map,
            evidence_limit=evidence_limit,
        )

    if not return_evidence_counts:
        return rv
    evidence_counts = {
        stmt.get_hash(): rel.data["evidence_count"] for rel, stmt in zip(rels, stmts)
    }
    return rv, evidence_counts

@autoclient()
def get_statements_mix(
    *,
    rel_type: Optional[str] = None,
    stmt_source: Optional[str] = None,
    agent_name: Optional[str] = None,
    agent_role: Optional[str] = None,
    limit: Optional[int] = 10,
    client: Neo4jClient,
    evidence_limit: Optional[int] = None
) -> List[Statement]:
    """Return the statements based on optional constraints on relationship type
    Parameters
    ----------
    rel_type : Optional[str], default: None
        The relationship type to query for (e.g., "Phosphorylation").
    stmt_source : Optional[str], default: None
        The source to query for (e.g., "reach").
    agent_name : Optional[str], default: None
        The name of the agent to filter by (e.g., "EGFR").
    agent_role : Optional[str], default: None
        The role of the agent in the interaction, either "subject" or "object".
    limit : Optional[int], default: 10
        The maximum number of statements returned
    client : Neo4jClient
        The Neo4j client to use for querying.
    evidence_limit : Optional[int], default: None
        The optional limit for the number of evidence entries per statement.

    Returns
    -------
    List[Statement]
        A list of statements filtered by the provided constraints.
    """

    if agent_name and agent_role:
        if agent_role.lower() == "subject":
            match_clause = f"(a:BioEntity {{name: $agent_name}})-[r:indra_rel"
            match_direction = "]->(b:BioEntity)"
        elif agent_role.lower() == "object":
            match_clause = f"(a:BioEntity)-[r:indra_rel"
            match_direction = f"]->(b:BioEntity {{name: $agent_name}})"
        else:
            raise ValueError("agent_role must be 'subject' or 'object'")
    else:
        match_clause = "(a:BioEntity)-[r:indra_rel"
        match_direction = "]->(b:BioEntity)"

    rel_constraints = []
    if rel_type:
        rel_constraints.append("stmt_type: $rel_type")

    if rel_constraints:
        match_clause += " {" + ", ".join(rel_constraints) + "}"

    query = f"MATCH p = {match_clause}{match_direction}"

    where_clauses = []
    if stmt_source:
        where_clauses.append(f'r.source_counts CONTAINS \'"{stmt_source}":\'')
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)

    query += " RETURN p LIMIT $limit"

    params = {
        "agent_name": agent_name,
        "rel_type": rel_type,
        "limit": limit
    }

    logger.info(f"Running query with constraints: rel_type={rel_type}, "
                f"source={stmt_source}, agent_name={agent_name}, "
                f"agent_role={agent_role}, limit={limit}")
    rels = client.query_relations(query, **params)
    stmts = indra_stmts_from_relations(rels, deduplicate=True)

    if evidence_limit and evidence_limit > 1:
        stmts = enrich_statements(
            stmts,
            client=client,
            evidence_limit=evidence_limit,
        )

    return stmts

@autoclient()
def get_stmts_for_agent_type(
    agent_name: str,
    agent_role: str,
    limit: Optional[int] = 10,
    *,
    client: Neo4jClient,
    evidence_limit: Optional[int] = None
) -> List[Statement]:
    """Return the statements for a given agent based on its role

    Parameters
    ----------
    agent_name : str
        The name of the agent (e.g.,"MEK").
    agent_role : str
        The role of the agent in the interaction, either "subject" or "object".
    limit : Optional[int], default: 10
        The maximum number of statements returned
    client : Neo4jClient
        The Neo4j client to use for querying.
    evidence_limit : Optional[int], default: None
        The optional limit for the number of evidence entries per statement.

    Returns
    -------
    List[Statement]
        A list of statements where the given agent acts as the subject or object.
    """

    # Construct the query based on the role of the agent
    if agent_role.lower() == "subject":
        query = """
        MATCH p = (a:BioEntity {name: $agent_name})-[r:indra_rel]->(b:BioEntity)
        RETURN p LIMIT $limit
        """
    elif agent_role.lower() == "object":
        query = """
        MATCH p = (a:BioEntity)-[r:indra_rel]->(b:BioEntity {name: $agent_name})
        RETURN p LIMIT $limit
        """
    else:
        query = """
        MATCH p = (a:BioEntity {name: $agent_name})-[r:indra_rel]-(b:BioEntity)
        RETURN p LIMIT $limit
        """

    params = {
        "agent_name": agent_name,
        "limit": limit
    }

    logger.info(f"Getting statements for agent '{agent_name}' with limit {limit}")
    rels = client.query_relations(query, **params)
    stmts = indra_stmts_from_relations(rels, deduplicate=True)
    if evidence_limit and evidence_limit > 1:
        stmts = enrich_statements(
            stmts,
            client=client,
            evidence_limit=evidence_limit,
        )
    return stmts

@autoclient()
def get_stmts_for_source(
        stmt_source: str,
        limit: Optional[int] = 10,
        *,
        client: Neo4jClient,
        evidence_limit: Optional[int] = None
) -> List[Statement]:
    """Return the statements for the given source.

    Parameters
    ----------
    stmt_source : str
        The source to query for (e.g., "reach").
    limit : Optional[int], default: 10
        The maximum number of statements to return.
    client : Neo4jClient
        The Neo4j client to use for querying.
    evidence_limit : Optional[int], default: None
        The optional limit for the number of evidence entries per statement.

    Returns
    -------
    List[Statement]
        A list of statements filtered by the given source.
    """
    query = """
    MATCH p = (a:BioEntity)-[r:indra_rel]->(b:BioEntity)
    WHERE r.source_counts CONTAINS '"' + $source + '":'
    RETURN p LIMIT $limit
    """

    params = {
        "source": stmt_source,
        "limit": limit
    }

    logger.info(f"Getting statements for source '{stmt_source}' with limit {limit}")
    rels = client.query_relations(query, **params)
    stmts = indra_stmts_from_relations(rels, deduplicate=True)
    if evidence_limit and evidence_limit > 1:
        stmts = enrich_statements(
            stmts,
            client=client,
            evidence_limit=evidence_limit,
        )
    return stmts


@autoclient()
def get_stmts_for_rel_type(
        rel_type: str,
        limit: Optional[int] = 10,
        *,
        client: Neo4jClient,
        evidence_limit: Optional[int] = None
) -> List[Statement]:
    """Return the statements for the given relationship type.

    Parameters
    ----------
    rel_type : str
        The relationship type to query for (e.g., "Phosphorylation").
    limit : Optional[int], default: 10
        The maximum number of statements returned
    client : Neo4jClient
        The Neo4j client to use for querying.
    evidence_limit : Optional[int], default: None
        The optional limit for the number of evidence entries per statement.

    Returns
    -------
    List[Statement]
        A list of statements filtered by the given relationship type.
    """

    query = """
    MATCH p = (a:BioEntity)-[r:indra_rel {stmt_type: $rel_type}]->(b:BioEntity)
    RETURN p LIMIT $limit
    """

    params = {
        "rel_type": rel_type,
        "limit": limit
    }

    logger.info(
        f"Getting statements for relationship type '{rel_type}' with limit {limit}")
    rels = client.query_relations(query, **params)
    stmts = indra_stmts_from_relations(rels, deduplicate=True)
    if evidence_limit and evidence_limit > 1:
        stmts = enrich_statements(
            stmts,
            client=client,
            evidence_limit=evidence_limit,
        )

    return stmts

@autoclient()
def enrich_statements(
    stmts: Sequence[Statement],
    *,
    client: Neo4jClient,
    evidence_map: Optional[Dict[int, List[Evidence]]] = None,
    evidence_limit: Optional[int] = None,
) -> List[Statement]:
    """Add additional evidence to the statements using the evidence graph."""
    # If the evidence_map is provided, check if it covers all the hashes
    # and if not, query for the evidence objects
    evidence_map: Dict[int, List[Evidence]] = evidence_map or {}
    missing_stmt_hashes: List[int] = sorted(
        {stmt.get_hash() for stmt in stmts}.difference(evidence_map)
    )

    # Get the evidence objects for the given statement hashes
    if missing_stmt_hashes:
        logger.info(f"looking up evidence for {len(missing_stmt_hashes)} statements")
        start_time = time.time()
        missing_evidences = get_evidences_for_stmt_hashes(
            missing_stmt_hashes,
            client=client,
            limit=evidence_limit,
        )
        evidence_count = sum(len(v) for v in missing_evidences.values())
        logger.info(
            f"got {evidence_count} evidences in {time.time() - start_time:.2f} seconds"
        )
        evidence_map.update(missing_evidences)

    logger.debug(f"Adding the evidence objects to {len(stmts)} statements")
    # if no result, keep the original statement evidence
    for stmt in stmts:
        stmt_hash = stmt.get_hash()
        ev_list: List[Evidence] = evidence_map.get(stmt_hash)
        if ev_list:
            stmt.evidence = ev_list
        else:
            logger.warning(
                f"No evidence for stmt hash {stmt_hash}, keeping sample evidence"
            )

    return stmts


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
    # todo: figure out why [:isa|partof*1..] is ~170x faster than [:isa*1..]
    #  for the query below
    query = (
        """
        MATCH (c:BioEntity)-[:isa|partof*1..]->(:BioEntity {id: $mesh_id})
        RETURN DISTINCT c.id
    """
    )
    return set(client.query_tx(query, squeeze=True, mesh_id=meshid_norm))


@autoclient(cache=True)
def get_node_counter(*, client: Neo4jClient) -> Counter:
    """Get a count of each entity type.

    Parameters
    ----------
    client :
        The Neo4j client.

    Returns
    -------
    :
        A Counter of the entity types.

        .. warning::

            This code assumes all nodes only have one label, as in ``label[0]``
    """
    return Counter(
        {
            label: client.query_tx(f"MATCH (n:{label}) RETURN count(*)", squeeze=True)[
                0
            ]
            for label in client.query_tx("call db.labels();", squeeze=True)
        }
    )


@autoclient(cache=True)
def get_prefix_counter(*, client: Neo4jClient) -> Counter:
    """Count node prefixes."""
    cypher = (
        """MATCH (n) WITH split(n.id, ":")[0] as prefix RETURN prefix, count(prefix)"""
    )
    return Counter(dict(client.query_tx(cypher)))


@autoclient(cache=True)
def get_edge_counter(*, client: Neo4jClient) -> Counter:
    """Get a count of each edge type."""
    return Counter(
        {
            relation: client.query_tx(
                f"MATCH ()-[r:{relation}]->() RETURN count(*)", squeeze=True
            )[0]
            for relation in client.query_tx(
                "call db.relationshipTypes();", squeeze=True
            )
        }
    )


@autoclient(cache=True)
def get_schema_graph(*, client: Neo4jClient) -> nx.MultiDiGraph:
    """Get a NetworkX graph reflecting the schema of the Neo4j graph.

    Generate a PDF diagram (works with PNG and SVG too) with the following::

    >>> from networkx.drawing.nx_agraph import to_agraph
    >>> client = ...
    >>> graph = get_schema_graph(client=client)
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


@autoclient()
def is_gene_mutated(
    gene: Tuple[str, str], cell_line: Tuple[str, str], *, client: Neo4jClient
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


@autoclient()
def get_mutated_genes(cell_line: Tuple[str, str], *, client: Neo4jClient) -> List[Node]:
    """Return the list of genes that are mutated in a given cell line.

    Parameters
    ----------
    client :
        The Neo4j client.
    cell_line :
        The cell line to query.

    Returns
    -------
    :
        The list of genes that are mutated in the given cell line.
    """
    return client.get_sources(
        target=cell_line,
        relation="mutated_in",
        source_type="BioEntity",
        target_type="BioEntity",
    )


# Indra DB


@autoclient()
def get_drugs_for_target(
    target: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Agent]:
    """Return the drugs targeting the given protein.

    Parameters
    ----------
    client :
        The Neo4j client.
    target :
        The target to query.

    Returns
    -------
    :
        The drugs targeting the given protein.
    """
    rels = client.get_source_relations(
        target, "indra_rel", source_type="BioEntity", target_type="BioEntity"
    )
    drug_rels = [rel for rel in rels if _is_drug_relation(rel)]
    drug_nodes = [
        _get_node_from_stmt_relation(rel, "source", "subj") for rel in drug_rels
    ]
    return drug_nodes


@autoclient()
def get_drugs_for_targets(
    targets: Iterable[Tuple[str, str]], *, client: Neo4jClient
) -> Mapping[str, Iterable[Agent]]:
    """Return the drugs targeting each of the given targets.

    Parameters
    ----------
    client :
        The Neo4j client.
    targets :
        The targets to query.

    Returns
    -------
    :
        A mapping of targets to the drugs targeting each of the given targets.
    """
    rels = client.get_source_relations_for_targets(
        targets, "indra_rel", source_type="BioEntity", target_type="BioEntity"
    )
    drug_nodes = {
        norm_id(*target): [
            _get_node_from_stmt_relation(rel, "source", "subj")
            for rel in target_rels
            if _is_drug_relation(rel)
        ]
        for target, target_rels in rels.items()
    }
    return drug_nodes


@autoclient()
def get_targets_for_drug(
    drug: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Agent]:
    """Return the proteins targeted by the given drug.

    Parameters
    ----------
    client :
        The Neo4j client.
    drug :
        The drug to query.

    Returns
    -------
    :
        The proteins targeted by the given drug.
    """
    rels = client.get_target_relations(
        drug, "indra_rel", source_type="BioEntity", target_type="BioEntity"
    )
    target_rels = [rel for rel in rels if _is_drug_relation(rel)]
    target_nodes = [
        _get_node_from_stmt_relation(rel, "target", "obj") for rel in target_rels
    ]
    return target_nodes


@autoclient()
def get_targets_for_drugs(
    drugs: Iterable[Tuple[str, str]], *, client: Neo4jClient
) -> Mapping[str, Iterable[Agent]]:
    """Return the proteins targeted by each of the given drugs

    Parameters
    ----------
    client :
        The Neo4j client.
    drugs :
        A list of drugs to get the targets for.

    Returns
    -------
    :
        A mapping from each drug to the proteins targeted by that drug.
    """
    rels = client.get_target_relations_for_sources(
        drugs, "indra_rel", source_type="BioEntity", target_type="BioEntity"
    )
    target_nodes = {
        norm_id(*drug): [
            _get_node_from_stmt_relation(rel, "target", "obj")
            for rel in drug_rels
            if _is_drug_relation(rel)
        ]
        for drug, drug_rels in rels.items()
    }
    return target_nodes


@autoclient()
def is_drug_target(
    drug: Tuple[str, str], target: Tuple[str, str], *, client: Neo4jClient
) -> bool:
    """Return True if the drug targets the given protein.

    Parameters
    ----------
    client :
        The Neo4j client.
    drug :
        The drug to query.
    target :
        The target to query.

    Returns
    -------
    :
        True if the drug targets the given protein.
    """
    rels = client.get_relations(
        drug, target, "indra_rel", source_type="BioEntity", target_type="BioEntity"
    )
    return any(_is_drug_relation(rel) for rel in rels)


def _get_ev_dict_from_hash_ev_query(
    result: Optional[Iterable[List[Union[int, str]]]] = None,
    remove_medscan: bool = True,
) -> Dict[int, List[Evidence]]:
    """Assumes `result` is an Iterable of pairs of [hash, evidence_json]"""
    if result is None:
        logger.warning("No result for hash, Evidence query, returning empty dict")
        return {}

    ev_dict = defaultdict(list)
    for stmt_hash, ev_json_str in result:
        ev_json = json.loads(ev_json_str)
        if remove_medscan and ev_json["source_api"] == "medscan":
            continue
        ev_dict[stmt_hash].append(Evidence._from_json(ev_json))
    return dict(ev_dict)


def _is_drug_relation(rel: Relation) -> bool:
    """Return True if the relation is a drug-target relation."""
    return rel.data["stmt_type"] == "Inhibition" and "tas" in rel.data["source_counts"]


def _get_node_from_stmt_relation(
    rel: Relation, node_role: str, agent_role: str
) -> Node:
    """Return the node from the given relation."""
    node_ns = getattr(rel, f"{node_role}_ns")
    node_id = getattr(rel, f"{node_role}_id")
    stmt_json = json.loads(rel.data["stmt_json"])
    name = stmt_json[agent_role]["name"]
    return Node(node_ns, node_id, ["BioEntity"], dict(name=name))


def _filter_out_medscan_evidence(
    ev_list: Iterable[Dict[str, Dict]], remove_medscan: bool = True
) -> List[Evidence]:
    """Filter out Evidence JSONs containing evidence from medscan."""
    return [
        Evidence._from_json(ev)
        for ev in ev_list
        if not (remove_medscan and ev["source_api"] == "medscan")
    ]


if __name__ == "__main__":
    print(get_prefix_counter())
    print(get_node_counter())
    print(get_edge_counter())
