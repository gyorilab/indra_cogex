from typing import Iterable, Tuple
from .neo4j_client import Neo4jClient
from ..representation import Node

# BGee


def get_expressed_genes_in_tissue(
    client: Neo4jClient, tissue: Tuple[str, str]
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
    return client.get_sources(tissue, relation="expressed_in")


def get_tissues_gene_expressed(client: Neo4jClient, gene: Tuple[str, str]):
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
    return client.get_targets(gene, relation="expressed_in")


def is_gene_expressed_in_tissue(
    client: Neo4jClient, gene: Tuple[str, str], tissue: Tuple[str, str]
):
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
    return client.has_relation(gene, tissue, relation="expressed_in")


# GO


def get_go_terms_for_gene(client: Neo4jClient, gene: Tuple[str, str]):
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
    return client.get_targets(gene, relation="associated_with")


def get_genes_for_go_term(
    client: Neo4jClient, go_term: Tuple[str, str], include_indirect=False
):
    """Return the genes associated with the given GO term.

    Parameters
    ----------
    client :
        The Neo4j client.
    go_term :
        The GO term to query.

    Returns
    -------
    :
        The genes associated with the given GO term.
    """
    go_children = get_ontology_child_terms(client, go_term) if include_indirect else []
    gene_nodes = {}
    for term in [go_term] + go_children:
        genes = client.get_sources(go_term, relation="associated_with")
        for gene in genes:
            gene_nodes[(gene.db_ns, gene.db_id)] = gene
    return list(gene_nodes.values())


def is_go_term_for_gene(
    client: Neo4jClient, gene: Tuple[str, str], go_term: Tuple[str, str]
):
    return client.is_connected(gene, go_term, relation="associated_with")


# Trials


def get_trials_for_drug(client: Neo4jClient, drug: Tuple[str, str]):
    return client.get_targets(drug, relation="tested_in")


def get_trials_for_disease(client: Neo4jClient, disease: Tuple[str, str]):
    return client.get_targets(disease, relation="has_trial")


def get_drugs_for_trial(client: Neo4jClient, trial: Tuple[str, str]):
    return client.get_sources(trial, relation="tested_in")


def get_diseases_for_trial(client: Neo4jClient, trial: Tuple[str, str]):
    return client.get_sources(trial, relation="has_trial")


# Pathways


def get_pathways_for_gene(client: Neo4jClient, gene: Tuple[str, str]):
    return client.get_targets(gene, relation="has_pathway")


def get_genes_for_pathway(client: Neo4jClient, pathway: Tuple[str, str]):
    return client.get_targets(pathway, relation="haspart")


def is_gene_in_pathway(
    client: Neo4jClient, gene: Tuple[str, str], pathway: Tuple[str, str]
):
    return client.has_relation(gene, pathway, relation="haspart")


# Side effects


def get_side_effects_for_drug(client: Neo4jClient, drug: Tuple[str, str]):
    return client.get_targets(drug, relation="has_side_effect")


def get_drugs_for_side_effect(client: Neo4jClient, side_effect: Tuple[str, str]):
    return client.get_sources(side_effect, relation="has_side_effect")


def is_side_effect_for_drug(
    client: Neo4jClient, drug: Tuple[str, str], side_effect: Tuple[str, str]
):
    return client.has_relation(drug, side_effect, relation="has_side_effect")


# Ontology


def get_ontology_child_terms(client: Neo4jClient, term: Tuple[str, str]):
    return client.get_predecessors(term, relations={"is_a", "partof"})


def get_ontology_parent_terms(client: Neo4jClient, term: Tuple[str, str]):
    return client.get_successors(term, relations={"is_a", "partof"})


def isa_or_partof(client: Neo4jClient, term: Tuple[str, str], parent: Tuple[str, str]):
    pass
