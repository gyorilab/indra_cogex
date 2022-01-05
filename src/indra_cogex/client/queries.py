import json
import logging
from collections import defaultdict
from typing import Iterable, Tuple, Mapping, Dict, List, Optional
from indra.statements import Evidence, Statement
from .neo4j_client import Neo4jClient
from ..representation import Node, indra_stmts_from_relations, norm_id


logger = logging.getLogger(__name__)


# BGee


def get_genes_in_tissue(client: Neo4jClient, tissue: Tuple[str, str]) -> Iterable[Node]:
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


def get_tissues_for_gene(client: Neo4jClient, gene: Tuple[str, str]) -> Iterable[Node]:
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


def is_gene_in_tissue(
    client: Neo4jClient, gene: Tuple[str, str], tissue: Tuple[str, str]
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


def get_go_terms_for_gene(
    client: Neo4jClient, gene: Tuple[str, str], include_indirect=False
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


def get_genes_for_go_term(
    client: Neo4jClient, go_term: Tuple[str, str], include_indirect: bool = False
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


def is_go_term_for_gene(
    client: Neo4jClient, gene: Tuple[str, str], go_term: Tuple[str, str]
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


def get_trials_for_drug(client: Neo4jClient, drug: Tuple[str, str]) -> Iterable[Node]:
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


def get_trials_for_disease(
    client: Neo4jClient, disease: Tuple[str, str]
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


def get_drugs_for_trial(client: Neo4jClient, trial: Tuple[str, str]) -> Iterable[Node]:
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


def get_diseases_for_trial(
    client: Neo4jClient, trial: Tuple[str, str]
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


def get_pathways_for_gene(client: Neo4jClient, gene: Tuple[str, str]) -> Iterable[Node]:
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
    return client.get_targets(
        gene,
        relation="haspart",
        source_type="BioEntity",
        target_type="BioEntity",
    )


def get_genes_for_pathway(
    client: Neo4jClient, pathway: Tuple[str, str]
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


def is_gene_in_pathway(
    client: Neo4jClient, gene: Tuple[str, str], pathway: Tuple[str, str]
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
        gene,
        pathway,
        relation="haspart",
        source_type="BioEntity",
        target_type="BioEntity",
    )


# Side effects


def get_side_effects_for_drug(
    client: Neo4jClient, drug: Tuple[str, str]
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


def get_drugs_for_side_effect(
    client: Neo4jClient, side_effect: Tuple[str, str]
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


def is_side_effect_for_drug(
    client: Neo4jClient, drug: Tuple[str, str], side_effect: Tuple[str, str]
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


def get_ontology_child_terms(
    client: Neo4jClient, term: Tuple[str, str]
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


def get_ontology_parent_terms(
    client: Neo4jClient, term: Tuple[str, str]
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


def isa_or_partof(
    client: Neo4jClient, term: Tuple[str, str], parent: Tuple[str, str]
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
    term_parents = get_ontology_parent_terms(client, term)
    return any(parent == parent_term.grounding() for parent_term in term_parents)


# MESH / PMID


def get_pmids_for_mesh(
    client: Neo4jClient, mesh: Tuple[str, str], include_child_terms: bool = True
) -> Iterable[str]:
    """Return the PubMed IDs for the given MESH term.

    Parameters
    ----------
    client :
        The Neo4j client.
    mesh :
        The MESH term to query.
    include_child_terms :
        If True, also match against the child MESH terms of the given MESH
        term.

    Returns
    -------
    :
        The PubMed IDs for the given MESH term and, optionally, its child terms.
    """
    if mesh[0].lower() != "mesh":
        raise ValueError(f"Expected mesh term, got {':'.join(mesh)}")
    norm_mesh = norm_id(*mesh)

    # NOTE: we could use get_ontology_child_terms() here, but it's ~20 times
    # slower for this specific query, so we do an optimized query that is
    # basically equivalent instead.
    if include_child_terms:
        child_query = (
            """
            MATCH (c:BioEntity)-[:isa*1..]->(p:BioEntity)
            WHERE p.id = "%s"
            RETURN DISTINCT c.id
            """
            % norm_mesh
        )
        child_terms = {c[0] for c in client.query_tx(child_query)}
    else:
        child_terms = set()

    if child_terms:
        terms = {norm_mesh} | child_terms
        terms_str = ",".join(f'"{c}"' for c in terms)
        where_clause = "WHERE b.id IN [%s]" % terms_str
    else:
        where_clause = 'WHERE b.id = "%s"' % norm_mesh

    query = (
        """MATCH (k:Publication)-[r:annotated_with]->(b:BioEntity)
        %s
        RETURN k.id
    """
        % where_clause
    )
    return [r[0] for r in client.query_tx(query)]


def get_mesh_ids_for_pmid(client: Neo4jClient, pmid: Tuple[str, str]) -> Iterable[str]:
    """Return the MESH terms for the given PubMed ID.

    Parameters
    ----------
    client :
        The Neo4j client.
    pmid :
        The PubMed ID to query.

    Returns
    -------
    :
        The MESH terms for the given PubMed ID.
    """
    if pmid[0].lower() != "pmid":
        raise ValueError(f"Expected pmid term, got {':'.join(pmid)}")
    norm_pmid = norm_id(*pmid)

    query = (
        """
        MATCH p=(k:Publication)-[r:annotated_with]->(b:BioEntity)
        WHERE k.id = "%s"
        RETURN b.id
    """
        % norm_pmid
    )
    return [r[0] for r in client.query_tx(query) if r[0].startswith("mesh:")]


def get_evidence_obj_for_stmt_hash(
    client: Neo4jClient, stmt_hash: str
) -> Iterable[Evidence]:
    """Return the matching evidence objects for the given statement hash.

    Parameters
    ----------
    client :
        The Neo4j client.
    stmt_hash :
        The statement hash to query.

    Returns
    -------
    :
        The evidence object for the given statement hash.
    """
    query = (
        """
        MATCH (n:Evidence)
        WHERE n.stmt_hash = "%s"
        RETURN n.evidence
    """
        % stmt_hash
    )
    ev_jsons = [json.loads(r[0]) for r in client.query_tx(query)]
    return [Evidence._from_json(ev_json) for ev_json in ev_jsons]


def get_evidence_objects_for_stmt_hashes(
    client: Neo4jClient, stmt_hashes: Iterable[str]
) -> Mapping[str, Evidence]:
    """Return the matching evidence objects for the given statement hashes.

    Parameters
    ----------
    client :
        The Neo4j client.
    stmt_hashes :
        The statement hashes to query.

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
    ev_dict = defaultdict(list)
    for hash_str, ev_json_str in client.query_tx(query):
        ev_json = json.loads(ev_json_str)
        ev_dict[hash_str].append(Evidence._from_json(ev_json))

    return dict(ev_dict)


def get_stmts_for_pmid(
    client: Neo4jClient, pmid: Tuple[str, str]
) -> Iterable[Statement]:
    """Return the statements with evidence for the given PubMed ID.

    Parameters
    ----------
    client :
        The Neo4j client.
    pmid :
        The PubMed ID to query.

    Returns
    -------
    :
        The statements for the given PubMed ID.
    """
    # ToDo: Investigate if it's possible to do this in one query - see more
    #  details in the Neo4j documentation:
    #  https://neo4j.com/developer/cypher/subqueries/
    # Todo: Add filters: e.g. belief cutoff, sources, db supported only,
    #  stmt type
    pmid_norm = norm_id(*pmid)
    # First, get the hashes for the given PubMed ID
    hash_query = (
        """
        MATCH (e:Evidence)-[r:has_citation]->(n:Publication)
        WHERE n.id = "%s"
        RETURN e.stmt_hash
    """
        % pmid_norm
    )
    hashes = [r[0] for r in client.query_tx(hash_query)]
    return get_stmts_for_stmt_hashes(client, hashes)


def get_stmts_for_mesh_id(
    client: Neo4jClient, meshid: Tuple[str, str], include_child_terms: bool = True
) -> Iterable[Statement]:
    """Return the statements with evidence for the given MESH ID.

    Parameters
    ----------
    client :
        The Neo4j client.
    meshid :
        The MESH ID to query.
    include_child_terms :
        If True, also match against the children of the given MESH ID.

    Returns
    -------
    :
        The statements for the given MESH ID.
    """
    # Get the publications annotated with the given MESH ID
    pmids = get_pmids_for_mesh(client, meshid, include_child_terms)

    # Get the all evidences for the given pmids
    pubmed_str = ",".join(f'"{p}"' for p in pmids)
    query = (
        """
        MATCH (e:Evidence)-[r:has_citation]->(n:Publication)
        WHERE n.id IN [%s]
        RETURN DISTINCT e.stmt_hash
    """
        % pubmed_str
    )
    hashes = [r[0] for r in client.query_tx(query)]
    return get_stmts_for_stmt_hashes(client, hashes)


def get_stmts_for_stmt_hashes(
    client: Neo4jClient, stmt_hashes: Iterable[str]
) -> Iterable[Statement]:
    """Return the statements for the given statement hashes.

    Parameters
    ----------
    client :
        The Neo4j client.
    stmt_hashes :
        The statement hashes to query.

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

    # Get the evidence objects for the given statement hashes
    evidences = get_evidence_objects_for_stmt_hashes(client, stmt_hashes)

    # Add the evidence objects to the statements, if no result, keep the
    # original statement evidence
    for stmt_hash, stmt in stmts.items():
        ev_list = evidences.get(stmt_hash, [])
        if ev_list:
            stmt.evidence = ev_list
        else:
            logger.warning(f'No evidence for stmt hash {stmt_hash}, '
                           f'keeping sample evidence')

    return list(stmts.values())


def _get_ev_dict_from_hash_ev_query(result: Optional[Iterable[List[str]]] = None) -> Dict[str, Evidence]:
    """Assumes the result is an Iterable of pairs of [hash, evidence_json]"""
    if result is None:
        logger.warning('No result for hash, Evidence query, returning empty dict')
        return {}

    ev_dict = defaultdict(list)
    for hash_str, ev_json_str in result:
        ev_json = json.loads(ev_json_str)
        ev_dict[hash_str].append(Evidence._from_json(ev_json))
    return dict(ev_dict)
