import json
import logging
import pickle
import time
from collections import Counter, defaultdict
from textwrap import dedent
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple, Union, Any

import networkx as nx

from indra.assemblers.indranet import IndraNetAssembler
from indra.statements import Agent, Evidence, Statement
from indra.sources import SOURCE_INFO

from indra_cogex.apps.constants import AGENT_NAME_CACHE
from .neo4j_client import Neo4jClient, autoclient
from ..representation import Node, Relation, indra_stmts_from_relations, norm_id, generate_paper_clause

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
    "get_mesh_annotated_evidence",
    "get_evidences_for_mesh",
    "get_evidences_for_stmt_hash",
    "get_evidences_for_stmt_hashes",
    "get_stmts_for_paper",
    "get_stmts_for_pmids",
    "get_stmts_for_mesh",
    "get_stmts_meta_for_stmt_hashes",
    "get_stmts_for_stmt_hashes",
    "get_statements",
    "get_drugs_for_target",
    "get_drugs_for_targets",
    "get_targets_for_drug",
    "get_targets_for_drugs",
    "is_drug_target",
    "get_markers_for_cell_type",
    "get_cell_types_for_marker",
    "is_marker_for_cell_type",
    "get_phenotypes_for_disease",
    "get_diseases_for_phenotype",
    "has_phenotype",
    "get_genes_for_phenotype",
    "get_phenotypes_for_gene",
    "has_phenotype_gene",
    "get_publisher_for_journal",
    "get_journals_for_publisher",
    "is_journal_published_by",
    "get_journal_for_publication",
    "get_publications_for_journal",
    "is_published_in_journal",
    "get_diseases_for_gene",
    "get_genes_for_disease",
    "has_gene_disease_association",
    "get_diseases_for_variant",
    "get_variants_for_disease",
    "has_variant_disease_association",
    "get_genes_for_variant",
    "get_variants_for_gene",
    "has_variant_gene_association",
    "get_publications_for_project",
    "get_clinical_trials_for_project",
    "get_patents_for_project",
    "get_projects_for_publication",
    "get_projects_for_clinical_trial",
    "get_projects_for_patent",
    "get_domains_for_gene",
    "get_genes_for_domain",
    "gene_has_domain",
    "get_phenotypes_for_variant_gwas",
    "get_variants_for_phenotype_gwas",
    "has_variant_phenotype_association",
    "get_indications_for_drug",
    "get_drugs_for_indication",
    "drug_has_indication",
    "get_codependents_for_gene",
    "gene_has_codependency",
    "get_enzyme_activities_for_gene",
    "get_genes_for_enzyme_activity",
    "has_enzyme_activity",
    "get_cell_lines_with_mutation",
    "get_mutated_genes_in_cell_line",
    "is_gene_mutated_in_cell_line",
    "get_cell_lines_with_cna",
    "get_cna_genes_in_cell_line",
    "has_cna_in_cell_line",
    "get_drugs_for_sensitive_cell_line",
    "get_sensitive_cell_lines_for_drug",
    "is_cell_line_sensitive_to_drug",
    "get_network_for_paper",
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
    genes: List[Tuple[str, str]], *, client: Neo4jClient
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
def get_mesh_annotated_evidence(
    stmt_hashes: List[str],
    mesh_term: Tuple[str, str],
    include_child_terms: bool = True,
    *,
    client: Neo4jClient
) -> Dict[str, List[Dict[str, Any]]]:
    """Return Evidence data for a given MESH term and a given list of statement hashes.

    Parameters
    ----------
    client :
        The Neo4j client.
    stmt_hashes :
        The statement hashes to query evidence for.
    mesh_term :
        The MESH term to constrain evidences to.
    include_child_terms :
        If True, also match against the child MESH terms of the given MESH
        term.

    Returns
    -------
    :
        A dictionary keyed by statement hash with each value being a list of
        Evidence data dictionaries.
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
    mesh_terms = list({norm_mesh} | child_terms)

    stmt_hashes = [int(h) for h in stmt_hashes]
    # Statement -> Evidence -> Publication -> MeSH Term
    query = """
    MATCH (e:Evidence) -[:has_citation]-> (p:Publication) -[:annotated_with]-> (b:BioEntity)
    WHERE e.stmt_hash in $stmt_hashes
    AND b.id IN $mesh_terms
    RETURN e
    """
    query_params = {"mesh_terms": mesh_terms, "stmt_hashes": stmt_hashes}
    res = client.query_nodes(query, **query_params)
    ret = defaultdict(list)
    for ev in res:
        ret[str(ev.data['stmt_hash'])].append(ev.data)
    return dict(ret)


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
    mesh_term: Tuple[str, str], include_child_terms: bool = True, include_db_evidence: bool = True, *,
    client: Neo4jClient
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
    include_db_evidence :
        If True, include and prioritize database evidence. If False, exclude it.

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
        query_params["mesh_terms"] = list(match_terms)
    else:
        single_mesh_match = ' {id: $mesh_id}'
        where_clause = ""
        query_params["mesh_id"] = norm_mesh

    db_filter = "" if include_db_evidence else "WHERE NOT e.source_api = 'database'"

    query = f"""
        MATCH (e:Evidence)-[:has_citation]->(:Publication)-[:annotated_with]->(b:BioEntity%s)
        %s
        %s
        WITH DISTINCT e.stmt_hash AS hash, COUNT(e) AS evidence_count
        LIMIT 25 
        WITH collect(hash) as top_hashes
        MATCH (e:Evidence)
        WHERE e.stmt_hash IN top_hashes
        %s
        RETURN e.stmt_hash, e.evidence
    """ % (single_mesh_match, where_clause, db_filter, db_filter)

    result = client.query_tx(query, **query_params)
    return _get_ev_dict_from_hash_ev_query(result, remove_medscan=True)


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
    stmt_hashes: List[int],
    *,
    client: Neo4jClient,
    limit: Optional[str] = None,
    remove_medscan: bool = True,
    mesh_terms: Optional[List[str]] = None,
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
    mesh_terms:
        A list of MeSH term IDs to filter evidence by linked publications.

    Returns
    -------
    :
        A mapping of stmt hash to a list of evidence objects for the given
        statement hashes.
    """
    limit_box = "" if limit is None else f"[..{limit}]"

    mesh_filter, mesh_pattern = "", ""
    if mesh_terms:
        mesh_filter = "AND mesh_term.id IN $mesh_terms"
        mesh_pattern = "-[:has_citation]->(pub:Publication)-[:annotated_with]->(mesh_term:BioEntity)"

    query = f"""\
        MATCH (n:Evidence){mesh_pattern}
        WHERE
            n.stmt_hash IN $stmt_hashes
            AND n.source_api <> $source_api {mesh_filter}
        RETURN n.stmt_hash, collect(n.evidence){limit_box}
    """

    query_params = {
        "stmt_hashes": list(stmt_hashes),
        "source_api": "medscan",
    }
    if mesh_terms:
        query_params["mesh_terms"] = mesh_terms

    result = client.query_tx(query, **query_params)

    return {
        stmt_hash: _filter_out_medscan_evidence(
            (json.loads(evidence_str) for evidence_str in evidences),
            remove_medscan=remove_medscan,
        )
        for stmt_hash, evidences in result
    }


@autoclient()
def get_stmts_for_paper(
    paper_term: Tuple[str, str], *, client: Neo4jClient, include_db_evidence: bool = False, **kwargs,
) -> List[Statement]:
    """Return the statements with evidence from the given PubMed ID.

    Parameters
    ----------
    client :
        The Neo4j client.
    paper_term :
        The term to query. Can be a PubMed ID, PMC id, TRID, or DOI
    include_db_evidence:
        Whether to include statements with database evidence.

    Returns
    -------
    :
        The statements for the given PubMed ID.
    """
    # Get just the database sources from SOURCE_INFO
    db_sources = {k for k, v in SOURCE_INFO.items() if v["type"] == "database"}

    parameter, publication_props = generate_paper_clause(paper_term)

    # Build WHERE clause to filter out all database sources
    if not include_db_evidence:
        # Create conditions to exclude all database sources
        db_conditions = [f"NOT e.evidence CONTAINS '\"{source}\"'"
                         for source in db_sources]
        where_clause = (
            f"WHERE (e.evidence IS NULL OR ({' AND '.join(db_conditions)}))\n"
        )
    else:
        where_clause = ""

    hash_query = f"""\
            MATCH (e:Evidence)-[:has_citation]->(:Publication {publication_props})
            {where_clause}RETURN e.stmt_hash, e.evidence"""

    result = client.query_tx(hash_query, paper_parameter=parameter)
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
    evidence_limit: int = 10,
    include_db_evidence: bool = True,
    **kwargs,
) -> Union[Tuple[List[Statement], Mapping[int, int]], List[Statement]]:
    """Return the statements with evidence for the given MESH ID.

    Parameters
    ----------
    include_db_evidence :
        Whether to include db evidence or not
    evidence_limit :
        Maximum number of evidence per statement
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
    stmts = get_stmts_for_stmt_hashes(
        hashes,
        evidence_map=evidence_map,
        client=client,
        evidence_limit=evidence_limit,
        include_db_evidence=include_db_evidence,
        **kwargs,
    )
    return stmts


@autoclient()
def get_stmts_meta_for_stmt_hashes(
    stmt_hashes: List[int],
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
    stmt_hashes: List[int],
    *,
    evidence_map: Optional[Dict[int, List[Evidence]]] = None,
    client: Neo4jClient,
    evidence_limit: Optional[int] = None,
    return_evidence_counts: bool = False,
    subject_prefix: Optional[str] = None,
    object_prefix: Optional[str] = None,
    include_db_evidence: bool = True,
) -> Union[List[Statement], Tuple[List[Statement], Mapping[int, int]]]:
    """Return the statements for the given statement hashes.

    Parameters
    ----------
    include_db_evidence :
        If True, include statements with database evidence. If False, exclude them.
    object_prefix :
        Filter statements to only those where the object ID starts with this prefix
    subject_prefix :
        Filter statements to only those where the subject ID starts with this prefix
    evidence_limit :
        An optional maximum number of evidences to return
    client :
        The Neo4j client.
    evidence_map :
        Optionally provide a mapping of stmt hash to a list of evidence objects
    stmt_hashes :
        The statement hashes to query.
    return_evidence_counts :
        If True, returns a tuple of (statements, evidence_counts). If False, returns
        only statements.

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

    db_evidence_constraint = "" if include_db_evidence else "AND NOT r.has_database_evidence"

    stmts_query = f"""\
        MATCH p=(a:BioEntity)-[r:indra_rel]->(b:BioEntity)
        WHERE
            r.stmt_hash IN $stmt_hashes
            {subject_constraint}
            {object_constraint}
            {db_evidence_constraint}
        RETURN p
    """
    logger.info(f"get_stmts_for_stmt_hashes executing query with {len(stmt_hashes)} hashes")
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
        rel.data["stmt_hash"]: rel.data["evidence_count"] for rel in rels
    }

    return rv, evidence_counts


@autoclient()
def get_statements(
    agent: Union[str, Tuple[str, str]],
    *,
    client: Neo4jClient,
    rel_types: Optional[Union[str, List[str]]] = None,
    stmt_sources: Optional[Union[str, List[str]]] = None,
    agent_role: Optional[str] = None,
    other_agent: Optional[Union[str, Tuple[str, str]]] = None,
    other_role: Optional[str] = None,
    paper_term: Optional[Tuple[str, str]] = None,
    mesh_term: Optional[Tuple[str, str]] = None,
    include_child_terms: Optional[bool] = True,
    limit: Optional[int] = 10,
    evidence_limit: Optional[int] = None,
    return_source_counts: bool = False,
) -> Union[List[Statement], Tuple[List[Statement], Mapping[int, int]]]:
    """Return the statements based on optional constraints on relationship type and source(s).

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client used for executing the query.
    rel_types : Optional[Union[str, List[str]]], default: None
        The relationship type(s) to filter by, e.g., "Phosphorylation" or ["Phosphorylation", "Activation"].
    stmt_sources : Optional[Union[str, List[str]]], default: None
        The source(s) to filter by, e.g., "reach" or ["reach", "sparser"].
    agent : Union[str, Tuple[str, str]]
        The primary agent involved in the interaction. Can be specified as a name (e.g., "EGFR") or as a CURIE
        tuple (namespace, ID), such as ("MESH", "D051379").
    agent_role : Optional[str], default: None
        The role of agent in the interaction: either "subject", "object", or None for an undirected search.
    other_agent : Optional[Union[str, Tuple[str, str]]], default: None
        A secondary agent in the interaction, specified either as a name or CURIE tuple.
    other_role : Optional[str], default: None
        The role of other_agent in the interaction: either "subject", "object", or None.
    paper_term: Optional[Tuple[str, str]], default : None
        The paper filter. Can be a PubMed ID, PMC id, TRID, or DOI
    mesh_term : Optional[Tuple[str, str]], default : None
        The mesh_term filter for evidences
    include_child_terms : Optional[bool], default : True
        If True, also match against the child MESH terms of the given MESH term.
    limit : Optional[int], default: 10
        The maximum number of statements to return.
    evidence_limit : Optional[int], default: None
        The optional maximum number of evidence entries to retrieve per statement.
    return_source_counts : bool, default: False
        Whether to include a mapping of statement hash to source counts in the results.

    Returns
    -------
    List[Statement]
        A list of statements filtered by the provided constraints.
    """
    where_clauses = []
    mesh_all_term, hash_in_rel = None, ""
    if paper_term:
        paper_param, paper_clause = generate_paper_clause(paper_term)
    else:
        paper_clause = ""

    if mesh_term or paper_term:
        query = (f"MATCH (e:Evidence)-[:has_citation]->"
                 f"(pub:Publication {paper_clause})-[:annotated_with] "
                 f"-> (mesh_term:BioEntity)")
        hash_in_rel = "{stmt_hash: e.stmt_hash}"
        if mesh_term:
            norm_mesh = norm_id(*mesh_term)
            if include_child_terms:
                child_terms = _get_mesh_child_terms(mesh_term, client=client)
            else:
                child_terms = set()
            if child_terms:
                mesh_all_term = {norm_mesh} | child_terms
                mesh_all_term = list(mesh_all_term)
            else:
                mesh_all_term = [norm_mesh]
            where_clauses.append("mesh_term.id IN $mesh_terms")
    else:
        query = ""

    # Agent being CURIE
    if isinstance(agent, tuple):
        agent_constraint = norm_id(*agent)
        agent_match_clause = f"(a:BioEntity {{id: $agent_constraint}})"
    # Agent being text name
    else:
        agent_constraint = agent
        agent_match_clause = f"(a:BioEntity {{name: $agent_constraint}})"

    if isinstance(other_agent, tuple):
        other_agent_constraint = norm_id(*other_agent)
        other_agent_match_clause = f"(b:BioEntity {{id: $other_agent_constraint}})"
    elif other_agent:
        other_agent_constraint = other_agent
        other_agent_match_clause = f"(b:BioEntity {{name: $other_agent_constraint}})"
    else:
        other_agent_match_clause = "(b:BioEntity)"

    if agent_role == "subject" and other_role == "object":
        match_clause = f"{agent_match_clause}-[r:indra_rel {hash_in_rel}]->{other_agent_match_clause}"
    elif agent_role == "object" and other_role == "subject":
        match_clause = f"{other_agent_match_clause}-[r:indra_rel {hash_in_rel}]->{agent_match_clause}"
    elif agent_role == "subject":
        match_clause = f"{agent_match_clause}-[r:indra_rel {hash_in_rel}]->{other_agent_match_clause}"
    elif agent_role == "object":
        match_clause = f"{other_agent_match_clause}-[r:indra_rel {hash_in_rel}]->{agent_match_clause}"
    else:
        match_clause = f"{agent_match_clause}-[r:indra_rel {hash_in_rel}]-{other_agent_match_clause}"

    if rel_types:
        if isinstance(rel_types, str):
            rel_types = [rel_types]
        where_clauses.append("r.stmt_type IN $rel_types")

    if stmt_sources:
        if isinstance(stmt_sources, str):
            stmt_sources = [stmt_sources]
        where_clauses.append("any(source IN $stmt_sources WHERE r.source_counts CONTAINS source)")

    if where_clauses:
        match_clause += " WHERE " + " AND ".join(where_clauses)

    query += f"""
        MATCH p = {match_clause}
        WITH distinct r.stmt_hash AS hash, r.evidence_count as ev_count, collect(p) as pp
        RETURN pp
        ORDER BY ev_count DESC
        LIMIT $limit
    """
    params = {
        "agent_constraint": agent_constraint,
        "rel_types": rel_types if isinstance(rel_types, list) else [rel_types],
        "limit": limit
    }
    if other_agent:
        params["other_agent_constraint"] = other_agent_constraint
    if mesh_all_term:
        params["mesh_terms"] = mesh_all_term
    if stmt_sources:
        params['stmt_sources'] = stmt_sources
    if paper_term:
        params['paper_parameter'] = paper_param

    logger.info(f"Running query with constraints: rel_type={rel_types}, "
                f"source={stmt_sources}, agent={agent}, other_agent={other_agent}, "
                f"mesh = {mesh_all_term}"
                f"agent_role={agent_role}, other_role={other_role}, limit={limit}")
    logger.info(query)
    rels = client.query_tx(query, **params)
    flattened_rels = [client.neo4j_to_relation(i[0]) for rel in rels for i in rel]
    stmts = indra_stmts_from_relations(flattened_rels, deduplicate=True)
    if evidence_limit and evidence_limit > 1:
        stmts = enrich_statements(
            stmts,
            client=client,
            evidence_limit=evidence_limit,
            mesh_terms=mesh_all_term,
        )

    if not return_source_counts:
        return stmts

    source_counts = {
        int(rel.data["stmt_hash"]): json.loads(rel.data["source_counts"])
        for rel in flattened_rels
    }

    return stmts, source_counts


def check_agent_existence(
    agent: Union[str, Tuple[str, str]],
) -> Union[bool, None]:
    """Check if an agent exists in the database."""
    if AGENT_NAME_CACHE.exists():
        with open(AGENT_NAME_CACHE, 'rb') as f:
            agent_cache = pickle.load(f)
    else:
        return None
    if isinstance(agent, tuple):
        agent = norm_id(*agent)
        return agent in agent_cache
    else:
        return agent in agent_cache


@autoclient()
def enrich_statements(
    stmts: Sequence[Statement],
    *,
    client: Neo4jClient,
    evidence_map: Optional[Dict[int, List[Evidence]]] = None,
    evidence_limit: Optional[int] = None,
    mesh_terms: Optional[List[str]] = None,
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
            mesh_terms= mesh_terms,
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
    targets: List[Tuple[str, str]], *, client: Neo4jClient
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
    drugs: List[Tuple[str, str]], *, client: Neo4jClient
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
    result: Optional[List[List[Union[int, str]]]] = None,
    remove_medscan: bool = True,
) -> Dict[int, List[Evidence]]:
    """Assumes result is an Iterable of pairs of [hash, evidence_json]"""
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
    ev_list: List[Dict[str, Dict]], remove_medscan: bool = True
) -> List[Evidence]:
    """Filter out Evidence JSONs containing evidence from medscan."""
    return [
        Evidence._from_json(ev)
        for ev in ev_list
        if not (remove_medscan and ev["source_api"] == "medscan")
    ]


# Cell marker functions
@autoclient()
def get_markers_for_cell_type(
    cell_type: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return the markers associated with the given cell type.

    Parameters
    ----------
    client :
        The Neo4j client
    cell_type :
        The cell type to query (e.g., ("cl", "0000020"))

    Returns
    -------
    :
        The markers (genes) associated with the cell type
    """
    cell_id = f"{cell_type[0]}:{cell_type[1]}".lower()
    query = """
    MATCH (c:BioEntity {id: $cell_id})-[r:has_marker]->(m:BioEntity)
    RETURN m
    """
    return client.query_nodes(query, cell_id=cell_id)


@autoclient()
def get_cell_types_for_marker(
    marker: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return the cell types associated with the given marker.

    Parameters
    ----------
    client :
        The Neo4j client
    marker :
        The marker (gene) to query (e.g., ("HGNC", "11337"))

    Returns
    -------
    :
        The cell types associated with the marker
    """
    marker_id = f"{marker[0]}:{marker[1]}".lower()
    query = """
    MATCH (c:BioEntity)-[r:has_marker]->(m:BioEntity {id: $marker_id})
    RETURN c
    """
    return client.query_nodes(query, marker_id=marker_id)


@autoclient()
def is_marker_for_cell_type(
    marker: Tuple[str, str], cell_type: Tuple[str, str], *, client: Neo4jClient
) -> bool:
    """Return True if the marker is associated with the given cell type.

    Parameters
    ----------
    client :
        The Neo4j client
    marker :
        The marker to query
    cell_type :
        The cell type to query

    Returns
    -------
    :
        True if the marker is associated with the cell type
    """
    marker_id = f"{marker[0]}:{marker[1]}".lower()
    cell_id = f"{cell_type[0]}:{cell_type[1]}".lower()
    query = """
    MATCH (c:BioEntity {id: $cell_id})-[r:has_marker]->(m:BioEntity {id: $marker_id})
    RETURN COUNT(r) > 0 as exists
    """
    return client.query_tx(query, cell_id=cell_id, marker_id=marker_id, squeeze=True)[0]


# HPOA Functions
@autoclient()
def get_phenotypes_for_disease(
    disease: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return the phenotypes associated with the given disease.

    Parameters
    ----------
    client :
        The Neo4j client
    disease :
        The disease to query (e.g., ("doid", "0040093"))

    Returns
    -------
    :
        The phenotypes associated with the disease
    """
    # Construct the full ID as it appears in database
    disease_id = f"{disease[0]}:{disease[1]}"

    # Use direct query since get_targets is returning an empty list
    query = f"""
    MATCH (d:BioEntity {{id: $disease_id}})-[r:has_phenotype]->(p:BioEntity)
    RETURN p
    """
    return client.query_nodes(query, disease_id=disease_id)


@autoclient()
def get_diseases_for_phenotype(
    phenotype: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return the diseases associated with the given phenotype.

    Parameters
    ----------
    client :
        The Neo4j client
    phenotype :
        The phenotype to query (e.g., ("hp", "0003138"))

    Returns
    -------
    :
        The diseases associated with the phenotype
    """
    phenotype_id = f"{phenotype[0]}:{phenotype[1]}".lower()
    query = """
    MATCH (d:BioEntity)-[r:has_phenotype]->(p:BioEntity {id: $phenotype_id})
    RETURN d
    """
    return client.query_nodes(query, phenotype_id=phenotype_id)


@autoclient()
def has_phenotype(
    disease: Tuple[str, str], phenotype: Tuple[str, str], *, client: Neo4jClient
) -> bool:
    """Return True if the disease has the given phenotype.

    Parameters
    ----------
    client :
        The Neo4j client
    disease :
        The disease to query
    phenotype :
        The phenotype to query

    Returns
    -------
    :
        True if the disease has the phenotype
    """
    disease_id = f"{disease[0]}:{disease[1]}".lower()
    phenotype_id = f"{phenotype[0]}:{phenotype[1]}".lower()
    query = """
    MATCH (d:BioEntity {id: $disease_id})-[r:has_phenotype]->(p:BioEntity {id: $phenotype_id})
    RETURN COUNT(r) > 0
    """
    return client.query_tx(query, disease_id=disease_id, phenotype_id=phenotype_id, squeeze=True)[0]


@autoclient()
def get_genes_for_phenotype(
    phenotype: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return the genes associated with the given phenotype.

    Parameters
    ----------
    client :
        The Neo4j client
    phenotype :
        The phenotype to query (MESH ID)

    Returns
    -------
    :
        The genes (HGNC) associated with the phenotype
    """
    return client.get_targets(
        phenotype,
        relation="phenotype_has_gene",
        source_type="BioEntity",
        target_type="BioEntity",
    )


@autoclient()
def get_phenotypes_for_gene(
    gene: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return the phenotypes associated with the given gene.

    Parameters
    ----------
    client :
        The Neo4j client
    gene :
        The gene to query (HGNC ID)

    Returns
    -------
    :
        The phenotypes (MESH) associated with the gene
    """
    return client.get_sources(
        gene,
        relation="phenotype_has_gene",
        source_type="BioEntity",
        target_type="BioEntity",
    )


@autoclient()
def has_phenotype_gene(
    phenotype: Tuple[str, str], gene: Tuple[str, str], *, client: Neo4jClient
) -> bool:
    """Return True if the phenotype is associated with the given gene.

    Parameters
    ----------
    client :
        The Neo4j client
    phenotype :
        The phenotype to query (MESH ID)
    gene :
        The gene to query (HGNC ID)

    Returns
    -------
    :
        True if the phenotype is associated with the gene
    """
    return client.has_relation(
        phenotype,
        gene,
        relation="phenotype_has_gene",
        source_type="BioEntity",
        target_type="BioEntity",
    )


# Wikidata functions
@autoclient()
def get_publisher_for_journal(
    journal: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return the publisher for the given journal.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    journal : Tuple[str, str]
        The journal to query (e.g., ("nlm", "100972832"))

    Returns
    -------
    :
        The publisher nodes associated with the journal
    """
    journal_id = f"{journal[0]}:{journal[1]}".lower()
    query = """
    MATCH (j:Journal {id: $journal_id})-[r:published_by]->(p:Publisher)
    RETURN p
    """
    return client.query_nodes(query, journal_id=journal_id)


@autoclient()
def get_journals_for_publisher(
    publisher: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return the journals for the given publisher.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    publisher : Tuple[str, str]
        The publisher to query (e.g., ("isni", "0000000080461210"))

    Returns
    -------
    :
        The journal nodes published by this publisher
    """
    publisher_id = f"{publisher[0]}:{publisher[1]}".lower()
    query = """
    MATCH (j:Journal)-[r:published_by]->(p:Publisher {id: $publisher_id})
    RETURN j
    """
    return client.query_nodes(query, publisher_id=publisher_id)


@autoclient()
def is_journal_published_by(
    journal: Tuple[str, str], publisher: Tuple[str, str], *, client: Neo4jClient
) -> bool:
    """Check if a journal is published by a specific publisher.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    journal : Tuple[str, str]
        The journal to query (e.g., ("nlm", "100972832"))
    publisher : Tuple[str, str]
        The publisher to query (e.g., ("isni", "0000000031304729"))

    Returns
    -------
    :
        True if the journal is published by the given publisher
    """
    journal_id = f"{journal[0]}:{journal[1]}".lower()
    publisher_id = f"{publisher[0]}:{publisher[1]}".lower()
    query = """
    MATCH (j:Journal {id: $journal_id})-[r:published_by]->(p:Publisher {id: $publisher_id})
    RETURN COUNT(r) > 0 as exists
    """
    return client.query_tx(query, journal_id=journal_id, publisher_id=publisher_id, squeeze=True)[0]


@autoclient()
def get_journal_for_publication(
    publication: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return the journal where the publication was published.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    publication : Tuple[str, str]
        The publication to query (e.g., ("pubmed", "14334679"))

    Returns
    -------
    :
        The journal nodes where this publication was published
    """
    return client.get_targets(
        publication,
        relation="published_in",
        source_type="Publication",
        target_type="Journal"
    )


@autoclient()
def get_publications_for_journal(
    journal: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return the publications published in the given journal.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    journal : Tuple[str, str]
        The journal to query (e.g., ("nlm", "0000201"))

    Returns
    -------
    :
        The publication nodes published in this journal
    """
    return client.get_sources(
        journal,
        relation="published_in",
        source_type="Publication",
        target_type="Journal"
    )


@autoclient()
def is_published_in_journal(
    publication: Tuple[str, str], journal: Tuple[str, str], *, client: Neo4jClient
) -> bool:
    """Check if a publication was published in a specific journal.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    publication : Tuple[str, str]
        The publication to query (e.g., ("pubmed", "14334679"))
    journal : Tuple[str, str]
        The journal to query (e.g., ("nlm", "0000201"))

    Returns
    -------
    :
        True if the publication was published in the given journal
    """
    return client.has_relation(
        publication,
        journal,
        relation="published_in",
        source_type="Publication",
        target_type="Journal"
    )


@autoclient()
def get_diseases_for_gene(
    gene: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return diseases associated with the given gene.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    gene : Tuple[str, str]
        The gene to query (e.g., ("hgnc", "57"))

    Returns
    -------
    :
        Disease nodes (DOID or MESH) associated with this gene
    """
    return client.get_targets(
        gene,
        relation="gene_disease_association",
        source_type="BioEntity",
        target_type="BioEntity"
    )


@autoclient()
def get_genes_for_disease(
    disease: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return genes associated with the given disease.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    disease : Tuple[str, str]
        The disease to query (e.g., ("doid", "2738") or ("mesh", "D011561"))

    Returns
    -------
    :
        Gene nodes (HGNC) associated with this disease
    """
    return client.get_sources(
        disease,
        relation="gene_disease_association",
        source_type="BioEntity",
        target_type="BioEntity"
    )


@autoclient()
def get_diseases_for_variant(
    variant: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return diseases associated with the given variant.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    variant : Tuple[str, str]
        The variant to query (e.g., ("dbsnp", "rs74615166"))

    Returns
    -------
    :
        Disease nodes (DOID or UMLS) associated with this variant
    """
    return client.get_targets(
        variant,
        relation="variant_disease_association",
        source_type="BioEntity",
        target_type="BioEntity"
    )


@autoclient()
def has_gene_disease_association(
    gene: Tuple[str, str], disease: Tuple[str, str], *, client: Neo4jClient
) -> bool:
    """Check if a gene is associated with a disease.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    gene : Tuple[str, str]
        The gene to query (e.g., ("hgnc", "57"))
    disease : Tuple[str, str]
        The disease to query (e.g., ("doid", "DOID:2738"))

    Returns
    -------
    :
        True if the gene is associated with the disease
    """
    return client.has_relation(
        gene,
        disease,
        relation="gene_disease_association",
        source_type="BioEntity",
        target_type="BioEntity"
    )


@autoclient()
def get_variants_for_disease(
    disease: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return variants associated with the given disease.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    disease : Tuple[str, str]
        The disease to query (e.g., ("doid", "10652") or ("umls", "C4528257"))

    Returns
    -------
    :
        Variant nodes (DBSNP) associated with this disease
    """
    return client.get_sources(
        disease,
        relation="variant_disease_association",
        source_type="BioEntity",
        target_type="BioEntity"
    )


@autoclient()
def has_variant_disease_association(
    variant: Tuple[str, str], disease: Tuple[str, str], *, client: Neo4jClient
) -> bool:
    """Check if a variant is associated with a disease.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    variant : Tuple[str, str]
        The variant to query (e.g., ("dbsnp", "rs9994441"))
    disease : Tuple[str, str]
        The disease to query (e.g., ("doid", "DOID:10652"))

    Returns
    -------
    :
        True if the variant is associated with the disease
    """
    return client.has_relation(
        variant,
        disease,
        relation="variant_disease_association",
        source_type="BioEntity",
        target_type="BioEntity"
    )


@autoclient()
def get_genes_for_variant(
    variant: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return genes associated with the given variant.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    variant : Tuple[str, str]
        The variant to query (e.g., ("dbsnp", "rs74615166"))

    Returns
    -------
    :
        Gene nodes (HGNC) associated with this variant
    """
    return client.get_targets(
        variant,
        relation="variant_gene_association",
        source_type="BioEntity",
        target_type="BioEntity"
    )


@autoclient()
def get_variants_for_gene(
    gene: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return variants associated with the given gene.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    gene : Tuple[str, str]
        The gene to query (e.g., ("hgnc", "12310"))

    Returns
    -------
    :
        Variant nodes (DBSNP) associated with this gene
    """
    return client.get_sources(
        gene,
        relation="variant_gene_association",
        source_type="BioEntity",
        target_type="BioEntity"
    )


@autoclient()
def has_variant_gene_association(
    variant: Tuple[str, str], gene: Tuple[str, str], *, client: Neo4jClient
) -> bool:
    """Check if a variant is associated with a gene.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    variant : Tuple[str, str]
        The variant to query (e.g., ("dbsnp", "rs74615166"))
    gene : Tuple[str, str]
        The gene to query (e.g., ("hgnc", "12310"))

    Returns
    -------
    :
        True if the variant is associated with the gene
    """
    return client.has_relation(
        variant,
        gene,
        relation="variant_gene_association",
        source_type="BioEntity",
        target_type="BioEntity"
    )


# nih_reporter
@autoclient()
def get_publications_for_project(
    project: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return publications associated with an NIH research project.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    project : Tuple[str, str]
        The project to query (e.g., ("nihreporter.project", "2106659"))

    Returns
    -------
    :
        Publication nodes associated with this project
    """
    return client.get_targets(
        project,
        relation="has_publication",
        source_type="ResearchProject",
        target_type="Publication"
    )


@autoclient()
def get_projects_for_publication(
    publication: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return NIH research projects associated with a publication.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    publication : Tuple[str, str]
        The publication to query (e.g., ("pubmed", "11818301"))

    Returns
    -------
    :
        Research project nodes associated with this publication
    """
    return client.get_sources(
        publication,
        relation="has_publication",
        source_type="ResearchProject",
        target_type="Publication"
    )


@autoclient()
def get_clinical_trials_for_project(
    project: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return clinical trials associated with an NIH research project.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    project : Tuple[str, str]
        The project to query (e.g., ("nihreporter.project", "6439077"))

    Returns
    -------
    :
        Clinical trial nodes associated with this project
    """
    return client.get_targets(
        project,
        relation="has_clinical_trial",
        source_type="ResearchProject",
        target_type="ClinicalTrial"
    )


@autoclient()
def get_projects_for_clinical_trial(
    trial: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return NIH research projects associated with a clinical trial.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    trial : Tuple[str, str]
        The clinical trial to query (e.g., ("clinicaltrials", "NCT00201240"))

    Returns
    -------
    :
        Research project nodes associated with this clinical trial
    """
    return client.get_sources(
        trial,
        relation="has_clinical_trial",
        source_type="ResearchProject",
        target_type="ClinicalTrial"
    )


@autoclient()
def get_patents_for_project(
    project: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return patents associated with an NIH research project.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    project : Tuple[str, str]
        The project to query (e.g., ("nihreporter.project", "2106676"))

    Returns
    -------
    :
        Patent nodes associated with this project
    """
    return client.get_targets(
        project,
        relation="has_patent",
        source_type="ResearchProject",
        target_type="Patent"
    )


@autoclient()
def get_projects_for_patent(
    patent: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return NIH research projects associated with a patent.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    patent : Tuple[str, str]
        The patent to query (e.g., ("google.patent", "US5939275"))

    Returns
    -------
    :
        Research project nodes associated with this patent
    """
    return client.get_sources(
        patent,
        relation="has_patent",
        source_type="ResearchProject",
        target_type="Patent"
    )


#interpro
@autoclient()
def get_domains_for_gene(
    gene: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return protein domains associated with the given gene.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    gene : Tuple[str, str]
        The gene to query (e.g., ("hgnc", "475"))

    Returns
    -------
    :
        Domain nodes (InterPro) associated with this gene
    """
    return client.get_targets(
        gene,
        relation="has_domain",
        source_type="BioEntity",
        target_type="BioEntity"
    )


@autoclient()
def get_genes_for_domain(
    domain: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return genes associated with the given protein domain.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    domain : Tuple[str, str]
        The domain to query (e.g., ("interpro", "IPR006047"))

    Returns
    -------
    :
        Gene nodes (HGNC) associated with this domain
    """
    return client.get_sources(
        domain,
        relation="has_domain",
        source_type="BioEntity",
        target_type="BioEntity"
    )


@autoclient()
def gene_has_domain(
    gene: Tuple[str, str], domain: Tuple[str, str], *, client: Neo4jClient
) -> bool:
    """Check if a gene has the given protein domain.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    gene : Tuple[str, str]
        The gene to query (e.g., ("hgnc", "475"))
    domain : Tuple[str, str]
        The domain to query (e.g., ("interpro", "IPR006047"))

    Returns
    -------
    :
        True if the gene has the given domain
    """
    return client.has_relation(
        gene,
        domain,
        relation="has_domain",
        source_type="BioEntity",
        target_type="BioEntity"
    )


#gwas
@autoclient()
def get_phenotypes_for_variant_gwas(
    variant: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return phenotypes associated with the given variant from GWAS.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    variant : Tuple[str, str]
        The variant to query (e.g., ("dbsnp", "rs13015548"))

    Returns
    -------
    :
        Phenotype nodes (MESH, EFO, or DOID) associated with this variant
    """
    return client.get_targets(
        variant,
        relation="variant_phenotype_association",
        source_type="BioEntity",
        target_type="BioEntity"
    )


@autoclient()
def get_variants_for_phenotype_gwas(
    phenotype: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return variants associated with the given phenotype from GWAS.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    phenotype : Tuple[str, str]
        The phenotype to query (e.g., ("mesh", "D001827"))

    Returns
    -------
    :
        Variant nodes (DBSNP) associated with this phenotype
    """
    return client.get_sources(
        phenotype,
        relation="variant_phenotype_association",
        source_type="BioEntity",
        target_type="BioEntity"
    )


@autoclient()
def has_variant_phenotype_association(
    variant: Tuple[str, str], phenotype: Tuple[str, str], *, client: Neo4jClient
) -> bool:
    """Check if a variant is associated with a phenotype in GWAS data.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    variant : Tuple[str, str]
        The variant to query (e.g., ("dbsnp", "rs13015548"))
    phenotype : Tuple[str, str]
        The phenotype to query (e.g., ("mesh", "D001827"))

    Returns
    -------
    :
        True if the variant is associated with the phenotype
    """
    return client.has_relation(
        variant,
        phenotype,
        relation="variant_phenotype_association",
        source_type="BioEntity",
        target_type="BioEntity"
    )


# chembl
@autoclient()
def get_indications_for_drug(
    molecule: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return indications associated with the given molecule from ChEMBL.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    molecule : Tuple[str, str]
        The molecule to query (e.g., ("chebi", "10001"))

    Returns
    -------
    :
        Disease nodes (MESH) associated with this molecule
    """
    molecule_id = f"{molecule[0]}:{molecule[1]}".lower()
    query = """
    MATCH (m:BioEntity {id: $molecule_id})-[r:has_indication]->(i:BioEntity)
    RETURN i
    """
    return client.query_nodes(query, molecule_id=molecule_id)


@autoclient()
def get_drugs_for_indication(
    indication: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return molecules associated with the given indication from ChEMBL.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    indication : Tuple[str, str]
        The disease indication to query (e.g., ("mesh", "D002318"))

    Returns
    -------
    :
        Molecule nodes (CHEBI or CHEMBL) associated with this indication
    """
    indication_id = f"{indication[0]}:{indication[1]}" if indication[
                                                              0].lower() == "mesh" else f"{indication[0]}:{indication[1]}".lower()
    query = """
    MATCH (m:BioEntity)-[r:has_indication]->(i:BioEntity {id: $indication_id})
    RETURN m
    """
    return client.query_nodes(query, indication_id=indication_id)


@autoclient()
def drug_has_indication(
    molecule: Tuple[str, str], indication: Tuple[str, str], *, client: Neo4jClient
) -> bool:
    """Check if a molecule is associated with an indication in ChEMBL data.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    molecule : Tuple[str, str]
        The molecule to query (e.g., ("chebi", "10001"))
    indication : Tuple[str, str]
        The disease indication to query (e.g., ("mesh", "D002318"))

    Returns
    -------
    :
        True if the molecule is associated with the indication
    """
    molecule_id = f"{molecule[0]}:{molecule[1]}".lower()
    indication_id = f"{indication[0]}:{indication[1]}" if indication[
                                                              0].lower() == "mesh" else f"{indication[0]}:{indication[1]}".lower()
    query = """
    MATCH (m:BioEntity {id: $molecule_id})-[r:has_indication]->(i:BioEntity {id: $indication_id})
    RETURN COUNT(r) > 0 as exists
    """
    return client.query_tx(query, molecule_id=molecule_id, indication_id=indication_id, squeeze=True)[0]


# depmap
@autoclient()
def get_codependents_for_gene(
    gene: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return genes that are codependent with the given gene from DepMap.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    gene : Tuple[str, str]
        The gene to query (e.g., ("hgnc", "1234"))

    Returns
    -------
    :
        Gene nodes that are codependent with the input gene
    """
    gene_id = f"{gene[0]}:{gene[1]}".lower()
    query = """
    MATCH (g1:BioEntity {id: $gene_id})-[r:codependent_with]->(g2:BioEntity)
    RETURN g2
    """
    return client.query_nodes(query, gene_id=gene_id)


@autoclient()
def gene_has_codependency(
    gene1: Tuple[str, str], gene2: Tuple[str, str], *, client: Neo4jClient
) -> bool:
    """Check if two genes are codependent according to DepMap data.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    gene1 : Tuple[str, str]
        First gene to query
    gene2 : Tuple[str, str]
        Second gene to query

    Returns
    -------
    :
        True if the genes are codependent
    """
    gene1_id = f"{gene1[0]}:{gene1[1]}".lower()
    gene2_id = f"{gene2[0]}:{gene2[1]}".lower()
    query = """
    MATCH (g1:BioEntity {id: $gene1_id})-[r:codependent_with]->(g2:BioEntity {id: $gene2_id})
    RETURN COUNT(r) > 0 as exists
    """
    return client.query_tx(query, gene1_id=gene1_id, gene2_id=gene2_id, squeeze=True)[0]


# ec
@autoclient()
def get_enzyme_activities_for_gene(
    gene: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return enzyme activities associated with the given gene.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    gene : Tuple[str, str]
        The gene to query (e.g., ("hgnc", "10007"))

    Returns
    -------
    :
        Enzyme activity nodes (ECCODE) associated with this gene
    """
    return client.get_targets(
        gene,
        relation="has_activity",
        source_type="BioEntity",
        target_type="BioEntity"
    )


@autoclient()
def get_genes_for_enzyme_activity(
    enzyme: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return genes associated with the given enzyme activity.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    enzyme : Tuple[str, str]
        The enzyme activity to query (e.g., ("ec-code", "3.4.21.105"))

    Returns
    -------
    :
        Gene nodes (HGNC) associated with this enzyme activity
    """
    return client.get_sources(
        enzyme,
        relation="has_activity",
        source_type="BioEntity",
        target_type="BioEntity"
    )


@autoclient()
def has_enzyme_activity(
    gene: Tuple[str, str], enzyme: Tuple[str, str], *, client: Neo4jClient
) -> bool:
    """Check if a gene has the given enzyme activity.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    gene : Tuple[str, str]
        The gene to query (e.g., ("hgnc", "10007"))
    enzyme : Tuple[str, str]
        The enzyme activity to query (e.g., ("ec-code", "3.4.21.105"))

    Returns
    -------
    :
        True if the gene has the given enzyme activity
    """
    return client.has_relation(
        gene,
        enzyme,
        relation="has_activity",
        source_type="BioEntity",
        target_type="BioEntity"
    )


# cbioportal
@autoclient()
def get_cell_lines_with_mutation(
    gene: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return cell lines where the given gene is mutated.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    gene : Tuple[str, str]
        The gene to query (e.g., ("hgnc", "11504"))

    Returns
    -------
    :
        Cell line nodes (CCLE) where this gene is mutated
    """
    return client.get_targets(
        gene,
        relation="mutated_in",
        source_type="BioEntity",
        target_type="BioEntity"
    )


@autoclient()
def get_mutated_genes_in_cell_line(
    cell_line: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return genes that are mutated in the given cell line.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    cell_line : Tuple[str, str]
        The cell line to query (e.g., ("ccle", "HEL_HAEMATOPOIETIC_AND_LYMPHOID_TISSUE"))

    Returns
    -------
    :
        Gene nodes (HGNC) that are mutated in this cell line
    """
    return client.get_sources(
        cell_line,
        relation="mutated_in",
        source_type="BioEntity",
        target_type="BioEntity"
    )


@autoclient()
def is_gene_mutated_in_cell_line(
    gene: Tuple[str, str], cell_line: Tuple[str, str], *, client: Neo4jClient
) -> bool:
    """Check if a gene is mutated in the given cell line.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    gene : Tuple[str, str]
        The gene to query (e.g., ("hgnc", "11504"))
    cell_line : Tuple[str, str]
        The cell line to query (e.g., ("ccle", "HEL_HAEMATOPOIETIC_AND_LYMPHOID_TISSUE"))

    Returns
    -------
    :
        True if the gene is mutated in the cell line
    """
    return client.has_relation(
        gene,
        cell_line,
        relation="mutated_in",
        source_type="BioEntity",
        target_type="BioEntity"
    )


@autoclient()
def get_cell_lines_with_cna(
    gene: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return cell lines where the given gene has copy number alteration.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    gene : Tuple[str, str]
        The gene to query (e.g., ("hgnc", "11216"))

    Returns
    -------
    :
        Cell line nodes (CCLE) where this gene has copy number alteration
    """
    return client.get_targets(
        gene,
        relation="copy_number_altered_in",
        source_type="BioEntity",
        target_type="BioEntity"
    )


@autoclient()
def get_cna_genes_in_cell_line(
    cell_line: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return genes that have copy number alteration in the given cell line.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    cell_line : Tuple[str, str]
        The cell line to query (e.g., ("ccle", "U266B1_HAEMATOPOIETIC_AND_LYMPHOID_TISSUE"))

    Returns
    -------
    :
        Gene nodes (HGNC) that have copy number alteration in this cell line
    """
    return client.get_sources(
        cell_line,
        relation="copy_number_altered_in",
        source_type="BioEntity",
        target_type="BioEntity"
    )


@autoclient()
def has_cna_in_cell_line(
    gene: Tuple[str, str], cell_line: Tuple[str, str], *, client: Neo4jClient
) -> bool:
    """Check if a gene has copy number alteration in the given cell line.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    gene : Tuple[str, str]
        The gene to query (e.g., ("hgnc", "11216"))
    cell_line : Tuple[str, str]
        The cell line to query (e.g., ("ccle", "U266B1_HAEMATOPOIETIC_AND_LYMPHOID_TISSUE"))

    Returns
    -------
    :
        True if the gene has copy number alteration in the cell line
    """
    return client.has_relation(
        gene,
        cell_line,
        relation="copy_number_altered_in",
        source_type="BioEntity",
        target_type="BioEntity"
    )


@autoclient()
def get_drugs_for_sensitive_cell_line(
    cell_line: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return drugs that the given cell line is sensitive to.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    cell_line : Tuple[str, str]
        The cell line to query (e.g., ("ccle", "RL952_ENDOMETRIUM"))

    Returns
    -------
    :
        Drug nodes (MESH or CHEBI) that this cell line is sensitive to
    """
    return client.get_targets(
        cell_line,
        relation="sensitive_to",
        source_type="BioEntity",
        target_type="BioEntity"
    )


@autoclient()
def get_sensitive_cell_lines_for_drug(
    drug: Tuple[str, str], *, client: Neo4jClient
) -> Iterable[Node]:
    """Return cell lines that are sensitive to the given drug.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    drug : Tuple[str, str]
        The drug to query (e.g., ("mesh", "C586365") or ("chebi", "131174"))

    Returns
    -------
    :
        Cell line nodes (CCLE) that are sensitive to this drug
    """
    return client.get_sources(
        drug,
        relation="sensitive_to",
        source_type="BioEntity",
        target_type="BioEntity"
    )


@autoclient()
def is_cell_line_sensitive_to_drug(
    cell_line: Tuple[str, str], drug: Tuple[str, str], *, client: Neo4jClient
) -> bool:
    """Check if a cell line is sensitive to the given drug.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client
    cell_line : Tuple[str, str]
        The cell line to query (e.g., ("ccle", "RL952_ENDOMETRIUM"))
    drug : Tuple[str, str]
        The drug to query (e.g., ("mesh", "C586365"))

    Returns
    -------
    :
        True if the cell line is sensitive to the drug
    """
    return client.has_relation(
        cell_line,
        drug,
        relation="sensitive_to",
        source_type="BioEntity",
        target_type="BioEntity"
    )


@autoclient()
def get_network_for_paper(
    paper_term: Tuple[str, str],
    include_db_evidence: bool = True,
    limit: int = 25,
    *,
    client: Neo4jClient,
) -> Dict:
    """Generate network visualization data for INDRA statements from a paper.

    Parameters
    ----------
    paper_term : Tuple[str, str]
        The paper identifier as a tuple of (prefix, id). Supported prefixes:
        - 'pubmed' or 'pmid': PubMed ID
        - 'doi': Digital Object Identifier
        - 'pmc': PubMed Central ID (will try to map to pubmed)
    include_db_evidence : bool, default=True
        Whether to include statements with database evidence.
    limit : int, default=25
        Maximum number of statements to include in the network.
    client : Neo4jClient
        The Neo4j client.

    Returns
    -------
    Dict
        A dictionary containing nodes and edges in vis.js format for network visualization.
    """
    # Handle prefix mapping based on test results
    prefix, identifier = paper_term
    prefix_lower = prefix.lower()

    # Map known prefixes to their correct form
    if prefix_lower == 'pmid':
        paper_term = ('pubmed', identifier)

    # Get statements for the paper
    statements = get_stmts_for_paper(
        paper_term,
        client=client,
        include_db_evidence=include_db_evidence
    )

    # Limit to top N statements
    statements = statements[:limit]

    # If no statements found, return empty network
    if not statements:
        return {"nodes": [], "edges": []}

    # Create network using IndraNetAssembler
    try:
        assembler = IndraNetAssembler(statements)
        graph = assembler.make_model(graph_type='multi_graph')

        # Find most connected node
        node_degrees = dict(graph.degree())
        central_node = max(node_degrees.items(), key=lambda x: x[1])[0]

        # Convert to vis.js format
        nodes = []
        edges = []

        # Create a mapping of db_refs for all entities
        entity_db_refs = {}
        for stmt in statements:
            for agent in stmt.agent_list():
                if agent is not None and hasattr(agent, 'db_refs'):
                    entity_db_refs[agent.name] = agent.db_refs

        # Process nodes with better type detection
        for node_id in graph.nodes():
            # Default node properties
            node_type = "Other"
            color = "#607D8B"  # default gray-blue
            shape = "ellipse"  # default shape

            # Try to get more detailed type information
            db_refs = {}
            if node_id in entity_db_refs:
                db_refs = entity_db_refs[node_id]

                # Comprehensive categorization of entity types
                if 'HGNC' in db_refs or 'FPLX' in db_refs:
                    # Genes or gene families
                    node_type = "HGNC" if 'HGNC' in db_refs else "FPLX"
                    color = "#4CAF50"  # green for genes/proteins/families
                    shape = "box"
                elif 'CHEBI' in db_refs or 'PUBCHEM' in db_refs:
                    # Chemical entities
                    node_type = "CHEBI" if 'CHEBI' in db_refs else "PUBCHEM"
                    color = "#FF9800"  # orange for chemicals
                    shape = "diamond"
                elif 'GO' in db_refs:
                    node_type = "GO"
                    color = "#2196F3"  # blue for GO terms
                    shape = "hexagon"
                elif 'MESH' in db_refs:
                    node_type = "MESH"
                    color = "#9C27B0"  # purple for MESH terms
                    shape = "triangle"
                elif 'UP' in db_refs:
                    # UniProt IDs - also proteins
                    node_type = "UP"
                    color = "#4CAF50"  # same green as genes
                    shape = "box"
                else:
                    # If there's any db_ref, use the first one
                    if db_refs:
                        node_type = next(iter(db_refs.keys()), "Other")
                        # For any other database types, use a unique color
                        if node_type != "Other":
                            color = "#009688"  # teal for other known types

            # Make central node more prominent
            if node_id == central_node:
                size = 45
                font_size = 26  # Increased font size
                border_width = 3
            else:
                size = 35
                font_size = 22  # Increased font size
                border_width = 2

            nodes.append({
                'id': str(node_id),
                'label': str(node_id),
                'title': f"{node_id} ({node_type})",
                'color': {
                    'background': color,
                    'border': '#37474F'
                },
                'shape': shape,
                'size': size,
                'font': {
                    'size': font_size,
                    'color': '#000000',
                    'face': 'arial',
                    'strokeWidth': 0,
                    'vadjust': -40  # Increased distance from node
                },
                'borderWidth': border_width,
                'details': db_refs,
                'egid': db_refs.get('EGID', ''),
                'hgnc': db_refs.get('HGNC', ''),
                'type': node_type.lower() if node_type else 'protein',
                'uniprot': db_refs.get('UP', '')
            })

        # Process edges - handle multi_graph correctly
        edge_count = 0
        for source, target, key, data in graph.edges(data=True, keys=True):
            # Find the corresponding statement for this edge
            edge_stmt = None
            found_stmt_type = None

            # Try to get stmt_type directly from edge data
            if 'stmt_type' in data:
                found_stmt_type = data['stmt_type']

            # Search for the statement that connects these nodes
            for stmt in statements:
                source_found = False
                target_found = False

                for agent in stmt.agent_list():
                    if agent is not None:
                        if agent.name == source:
                            source_found = True
                        elif agent.name == target:
                            target_found = True

                if source_found and target_found:
                    edge_stmt = stmt
                    found_stmt_type = type(stmt).__name__
                    break

            # Extract statement type
            stmt_type = found_stmt_type if found_stmt_type else 'Interaction'

            # Determine color and style based on statement type
            # Make color differences more dramatic
            dashes = False  # solid line by default
            arrows = {
                'to': {'enabled': True, 'scaleFactor': 0.5}  # Default arrow
            }
            width = 4  # Default width

            if 'Activation' in stmt_type:
                color = '#00CC00'  # bright green
            elif 'Inhibition' in stmt_type:
                color = '#FF0000'  # bright red
            elif 'Phosphorylation' in stmt_type:
                color = '#000000'  # black
            elif 'Complex' in stmt_type:
                color = '#0000FF'  # bright blue
                arrows = {
                    'to': {'enabled': False},
                    'from': {'enabled': False}
                }
            elif 'IncreaseAmount' in stmt_type:
                color = '#00CC00'  # bright green (same as Activation)
                dashes = [5, 5]  # dashed line
            elif 'DecreaseAmount' in stmt_type:
                color = '#FF0000'  # bright red (same as Inhibition)
                dashes = [5, 5]  # dashed line
            else:
                # Default color and width
                color = '#999999'  # gray
                width = 3

            # Get belief score
            belief = 0.5
            if edge_stmt and hasattr(edge_stmt, 'belief'):
                belief = edge_stmt.belief
            elif 'belief' in data:
                belief = data['belief']

            # Collect edge details for tooltip/dialog
            edge_details = {
                'statement_type': stmt_type,
                'belief': belief,
                'indra_statement': str(edge_stmt) if edge_stmt else 'Unknown',
                'interaction': stmt_type.lower(),
                'polarity': 'positive' if 'Activation' in stmt_type or 'IncreaseAmount' in stmt_type else
                'negative' if 'Inhibition' in stmt_type or 'DecreaseAmount' in stmt_type else 'none',
                'support_type': 'database' if include_db_evidence else 'literature',
                'type': stmt_type
            }

            # Then set these properties directly on the edge object
            edges.append({
                'id': f"e{edge_count}",
                'from': str(source),
                'to': str(target),
                'title': stmt_type,
                'color': {
                    'color': color,
                    'highlight': color,
                    'hover': color
                },  # Make color an object with multiple properties
                'dashes': dashes,
                'arrows': arrows,
                'width': width,
                'details': edge_details,
                'label': ''
            })
            edge_count += 1

        return {
            'nodes': nodes,
            'edges': edges
        }
    except Exception as e:
        import traceback
        traceback.print_exc()

        # Return empty network on error
        return {"nodes": [], "edges": []}


if __name__ == "__main__":
    print(get_prefix_counter())
    print(get_node_counter())
    print(get_edge_counter())
