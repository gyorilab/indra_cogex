# -*- coding: utf-8 -*-

"""A collection of analyses possible on gene lists (of HGNC identifiers)."""

from typing import Collection, Iterable, List, Mapping, Optional, Set, Tuple, Union
import logging
import numpy as np
import pandas as pd
from scipy.stats import fisher_exact
from statsmodels.stats.multitest import multipletests

from indra_cogex.apps.search.search import get_kinase_phosphosite_statements
from indra_cogex.client.enrichment.utils import (
    get_entity_to_regulators,
    get_entity_to_targets,
    get_go,
    get_phenotype_gene_sets,
    get_reactome,
    get_wikipathways,
    get_kinase_phosphosites,
)
from indra_cogex.client.neo4j_client import Neo4jClient, autoclient
from indra_cogex.client.queries import get_genes_for_go_term

logger = logging.getLogger(__name__)

__all__ = [
    "go_ora",
    "wikipathways_ora",
    "reactome_ora",
    "phenotype_ora",
    "indra_downstream_ora",
    "indra_upstream_ora",
    "kinase_ora",
    "EXAMPLE_GENE_IDS",
]

# fmt: off
#: This example list comes from human genes associated with COVID-19
#: (https://bgee.org/?page=top_anat#/result/9bbddda9dea22c21edcada56ad552a35cb8e29a7/)
EXAMPLE_GENE_IDS = [
    "613", "1116", "1119", "1697", "7067", "2537", "2734", "29517", "8568", "4910", "4931", "4932", "4962", "4983",
    "18873", "5432", "5433", "5981", "16404", "5985", "18358", "6018", "6019", "6021", "6118", "6120", "6122",
    "6148", "6374", "6378", "6395", "6727", "14374", "8004", "18669", "8912", "30306", "23785", "9253", "9788",
    "10498", "10819", "6769", "11120", "11133", "11432", "11584", "18348", "11849", "28948", "11876", "11878",
    "11985", "20820", "12647", "20593", "12713"
]


# fmt: on


def _prepare_hypergeometric_test(
    query_set: Set[str],
    target_set: Set[str],
    universe_size: int,
) -> np.ndarray:
    """Prepare the matrix for hypergeometric test calculations.

    Parameters
    ----------
    query_set:
        Input gene set to test against a target set (e.g., a pathway).
    target_set:
        The target gene set (e.g., a pathway).
    universe_size:
        Size of the background gene set.

    Returns
    -------
    :
        A 2x2 matrix used as input for Fisher's exact test.
    """
    return np.array(
        [
            [
                len(query_set.intersection(target_set)),
                len(query_set.difference(target_set)),
            ],
            [
                len(target_set.difference(query_set)),
                universe_size - len(target_set.union(query_set)),
            ],
        ]
    )


@autoclient(cache=True)
def count_human_genes(*, client: Neo4jClient) -> int:
    """Count the number of HGNC genes in neo4j.

    Parameters
    ----------
    client :
        Neo4jClient

    Returns
    -------
    :
        Number of HGNC genes
    """
    query = """\
        MATCH (n:BioEntity)
        WHERE n.id STARTS WITH 'hgnc'
        AND NOT n.obsolete
        RETURN count(n) as count
    """
    results = client.query_tx(query)
    if results is None:
        raise ValueError
    return results[0][0]


def gene_ontology_single_ora(
    client: Neo4jClient,
    go_term: Tuple[str, str],
    gene_ids: List[str],
) -> float:
    """Get the *p*-value for the Fisher exact test a given GO term.

    1. Look up genes associated with GO term or child terms
    2. Run ORA and return results

    Parameters
    ----------
    client :
        Neo4jClient
    go_term :
        GO term to test
    gene_ids :
        List of HGNC gene identifiers

    Returns
    -------
    :
        p-value
    """
    count = count_human_genes(client=client)
    go_gene_ids = {
        gene.db_id
        for gene in get_genes_for_go_term(
            client=client, go_term=go_term, include_indirect=True
        )
    }
    table = _prepare_hypergeometric_test(
        query_set=set(gene_ids),
        target_set=go_gene_ids,
        universe_size=count,
    )
    return fisher_exact(table, alternative="greater")[1]


def _do_ora(
    curie_to_target_sets: Mapping[Tuple[str, str], Set[Union[str, Tuple[str, str]]]],
    query: Iterable[Union[str, Tuple[str, str]]],
    count: int,
    method: Optional[str] = "fdr_bh",
    alpha: Optional[float] = None,
    keep_insignificant: bool = True,
) -> pd.DataFrame:
    if alpha is None:
        alpha = 0.05
    query_set = set(query)
    rows = []
    for (curie, name), target_set in curie_to_target_sets.items():
        table = _prepare_hypergeometric_test(
            query_set=query_set,
            target_set=target_set,
            universe_size=count,
        )
        _, pvalue = fisher_exact(table, alternative="greater")
        rows.append((curie, name, pvalue))
    df = pd.DataFrame(rows, columns=["curie", "name", "p"]).sort_values(
        "p", ascending=True
    )
    df["mlp"] = -np.log10(df["p"])
    if method:
        correction_results = multipletests(
            df["p"],
            method=method,
            is_sorted=True,
            alpha=alpha,
        )
        df["q"] = correction_results[1]
        df["mlq"] = -np.log10(df["q"])
        df = df.sort_values("q", ascending=True)
    if not keep_insignificant:
        df = df[df["q"] < alpha]
    return df


def go_ora(
    client: Neo4jClient,
    gene_ids: Iterable[str],
    background_gene_ids: Optional[Collection[str]] = None,
    **kwargs,
) -> pd.DataFrame:
    """Calculate over-representation on all GO terms.

    Parameters
    ----------
    client :
        Neo4jClient
    gene_ids :
        List of HGNC gene identifiers
    background_gene_ids :
        List of HGNC gene identifiers for the background gene set. If not
        given, all genes with HGNC IDs are used as the background.
    **kwargs :
        Additional keyword arguments to pass to _do_ora

    Returns
    -------
    :
        DataFrame with columns:
        curie, name, p, q, mlp, mlq
    """
    count = (
        count_human_genes(client=client)
        if not background_gene_ids
        else len(background_gene_ids)
    )
    bg_genes = frozenset(background_gene_ids) if background_gene_ids else None
    return _do_ora(get_go(client=client, background_gene_ids=bg_genes),
                   query=gene_ids, count=count, **kwargs)


def wikipathways_ora(
    client: Neo4jClient,
    gene_ids: Iterable[str],
    background_gene_ids: Optional[Collection[str]] = None,
    **kwargs,
) -> pd.DataFrame:
    """Calculate over-representation on all WikiPathway pathways.

    Parameters
    ----------
    client :
        Neo4jClient
    gene_ids :
        List of HGNC gene identifiers
    background_gene_ids :
        List of HGNC gene identifiers for the background gene set. If not
        given, all genes with HGNC IDs are used as the background.
    **kwargs :
        Additional keyword arguments to pass to _do_ora

    Returns
    -------
    :
        DataFrame with columns:
        curie, name, p, q, mlp, mlq
    """
    count = (
        count_human_genes(client=client)
        if not background_gene_ids
        else len(background_gene_ids)
    )
    bg_genes = frozenset(background_gene_ids) if background_gene_ids else None
    return _do_ora(
        get_wikipathways(client=client, background_gene_ids=bg_genes),
        query=gene_ids, count=count, **kwargs
    )


def reactome_ora(
    client: Neo4jClient,
    gene_ids: Iterable[str],
    background_gene_ids: Optional[Collection[str]] = None,
    **kwargs,
) -> pd.DataFrame:
    """Calculate over-representation on all Reactome pathways.

    Parameters
    ----------
    client :
        Neo4jClient
    gene_ids :
        List of HGNC gene identifiers
    background_gene_ids :
        List of HGNC gene identifiers for the background gene set. If not
        given, all genes with HGNC IDs are used as the background.
    **kwargs :
        Additional keyword arguments to pass to _do_ora

    Returns
    -------
    :
        DataFrame with columns:
        curie, name, p, q, mlp, mlq
    """
    count = (
        count_human_genes(client=client)
        if not background_gene_ids
        else len(background_gene_ids)
    )
    bg_genes = frozenset(background_gene_ids) if background_gene_ids else None
    return _do_ora(get_reactome(client=client, background_gene_ids=bg_genes),
                   query=gene_ids, count=count, **kwargs)


@autoclient()
def phenotype_ora(
    gene_ids: Iterable[str],
    background_gene_ids: Optional[Collection[str]] = None,
    *,
    client: Neo4jClient,
    **kwargs,
) -> pd.DataFrame:
    """Calculate over-representation on all HP phenotypes.

    Parameters
    ----------
    gene_ids :
        List of HGNC gene identifiers
    background_gene_ids :
        List of HGNC gene identifiers for the background gene set. If not
        given, all genes with HGNC IDs are used as the background.
    client :
        Neo4jClient
    **kwargs :
        Additional keyword arguments to pass to _do_ora

    Returns
    -------
    :
        DataFrame with columns:
        curie, name, p, q, mlp, mlq
    """
    count = (
        count_human_genes(client=client)
        if not background_gene_ids
        else len(background_gene_ids)
    )
    bg_genes = frozenset(background_gene_ids) if background_gene_ids else None
    return _do_ora(
        get_phenotype_gene_sets(client=client, background_gene_ids=bg_genes),
        query=gene_ids, count=count, **kwargs
    )


def indra_downstream_ora(
    client: Neo4jClient,
    gene_ids: Iterable[str],
    background_gene_ids: Optional[Collection[str]] = None,
    *,
    minimum_evidence_count: Optional[int] = 1,
    minimum_belief: Optional[float] = 0.0,
    **kwargs,
) -> pd.DataFrame:
    """
    Calculate a p-value for each entity in the INDRA database
    based on the genes that are causally upstream of it and how
    they compare to the query gene set.

    Parameters
    ----------
    client :
        Neo4jClient
    gene_ids :
        List of HGNC gene identifiers
    background_gene_ids :
        List of HGNC gene identifiers for the background gene set. If not
        given, all genes with HGNC IDs are used as the background.
    minimum_evidence_count :
        Minimum number of evidences to consider a causal relationship
    minimum_belief :
        Minimum belief to consider a causal relationship
    **kwargs :
        Additional keyword arguments to pass to _do_ora

    Returns
    -------
    :
        DataFrame with columns:
        curie, name, p, q, mlp, mlq
    """
    count = (
        count_human_genes(client=client)
        if not background_gene_ids
        else len(background_gene_ids)
    )
    bg_genes = frozenset(background_gene_ids) if background_gene_ids else None
    return _do_ora(
        get_entity_to_regulators(
            client=client,
            minimum_evidence_count=minimum_evidence_count,
            minimum_belief=minimum_belief,
            background_gene_ids=bg_genes
        ),
        query=gene_ids,
        count=count,
        **kwargs,
    )


def indra_upstream_ora(
    client: Neo4jClient,
    gene_ids: Iterable[str],
    background_gene_ids: Optional[Collection[str]] = None,
    *,
    minimum_evidence_count: Optional[int] = 1,
    minimum_belief: Optional[float] = 0.0,
    **kwargs,
) -> pd.DataFrame:
    """
    Calculate a p-value for each entity in the INDRA database
    based on the set of genes that it regulates and how
    they compare to the query gene set.

    Parameters
    ----------
    client :
        Neo4jClient
    gene_ids :
        List of HGNC gene identifiers
    background_gene_ids :
        List of HGNC gene identifiers for the background gene set. If not
        given, all genes with HGNC IDs are used as the background.
    minimum_evidence_count :
        Minimum number of evidences to consider a causal relationship
    minimum_belief :
        Minimum belief to consider a causal relationship
    **kwargs :
        Additional keyword arguments to pass to _do_ora

    Returns
    -------
    :
        DataFrame with columns:
        curie, name, p, q, mlp, mlq
    """
    count = (
        count_human_genes(client=client)
        if not background_gene_ids
        else len(background_gene_ids)
    )
    bg_genes = frozenset(background_gene_ids) if background_gene_ids else None
    return _do_ora(
        get_entity_to_targets(
            client=client,
            minimum_evidence_count=minimum_evidence_count,
            minimum_belief=minimum_belief,
            background_gene_ids=bg_genes
        ),
        query=gene_ids,
        count=count,
        **kwargs,
    )


@autoclient(cache=True)
def count_phosphosites(*, client: Neo4jClient) -> int:
    """Count the number of unique phosphosites in the Neo4j database.

    A phosphosite is defined as a unique combination of target protein,
    residue, and position. This function counts distinct phosphosites
    rather than all phosphorylation statements.

    Parameters
    ----------
    client :
        Neo4jClient

    Returns
    -------
    :
        Number of unique phosphosites
    """
    # Query to get all target, residue, position combinations
    query = """\
        MATCH (s:BioEntity)-[r:indra_rel]->(t:BioEntity) 
        WHERE r.stmt_type = 'Phosphorylation' 
          AND r.stmt_json CONTAINS '"residue"' 
          AND r.stmt_json CONTAINS '"position"'
        RETURN DISTINCT t.id as target_id, 
                        r.stmt_json as json_string
    """

    results = client.query_tx(query)

    # Process the results to extract unique phosphosites
    unique_sites = set()
    import json

    for target_id, json_string in results:
        try:
            stmt_json = json.loads(json_string)
            residue = stmt_json.get("residue")
            position = stmt_json.get("position")
            if residue and position:
                unique_sites.add((target_id, residue, position))
        except json.JSONDecodeError:
            continue

    count = len(unique_sites)

    if count == 0:
        # Fallback to a minimum value to avoid division by zero
        return 1000  # Arbitrary non-zero value

    return count


def kinase_ora(
    client: Neo4jClient,
    phosphosite_ids: Iterable[Tuple[str, str]],  # List of (gene_id, site) tuples
    background_phosphosite_ids: Optional[Collection[Tuple[str, str]]] = None,
    *,
    minimum_evidence_count: Optional[int] = 1,
    minimum_belief: Optional[float] = 0.0,
    **kwargs,
) -> pd.DataFrame:
    """Perform over-representation analysis on kinase-phosphosite relationships.

    Parameters
    ----------
    client :
        Neo4jClient
    phosphosite_ids :
        List of (gene, phosphosite) tuples.
    background_phosphosite_ids :
        List of (gene, phosphosite) tuples for the background set.
    minimum_evidence_count :
        Minimum number of evidences to consider a kinase-phosphosite relationship
    minimum_belief :
        Minimum belief score to consider a kinase-phosphosite relationship
    **kwargs :
        Additional keyword arguments to pass to _do_ora.

    Returns
    -------
    :
        DataFrame with columns:
        curie (kinase ID), name (kinase name), p (p-value), q (adjusted p-value), mlp (-log10 p), mlq (-log10 q).
    """
    phosphosite_ids = list(phosphosite_ids)  # Convert to list for multiple use

    count = (
        count_phosphosites(client=client)
        if not background_phosphosite_ids
        else len(background_phosphosite_ids)
    )

    bg_phosphosites = (
        frozenset(background_phosphosite_ids) if background_phosphosite_ids else None
    )

    kinase_to_phosphosites = get_kinase_phosphosites(
        client=client,
        background_phosphosites=bg_phosphosites,
        minimum_evidence_count=minimum_evidence_count,
        minimum_belief=minimum_belief
    )

    if not kinase_to_phosphosites:
        logger.warning("No kinase-phosphosite relationships found, returning empty DataFrame")
        return pd.DataFrame(columns=['curie', 'name', 'p', 'q', 'mlp', 'mlq'])

    # Check for overlap between query phosphosites and known phosphosite targets
    all_known_phosphosites = set()
    for phosphosites in kinase_to_phosphosites.values():
        all_known_phosphosites.update(phosphosites)

    overlap = [ps for ps in phosphosite_ids if ps in all_known_phosphosites]

    if not overlap:
        logger.warning("No overlap between query phosphosites and known targets, returning empty DataFrame")
        return pd.DataFrame(columns=['curie', 'name', 'p', 'q', 'mlp', 'mlq', 'statements'])

    # Perform ORA
    df = _do_ora(
        curie_to_target_sets=kinase_to_phosphosites,
        query=phosphosite_ids,
        count=count,
        **kwargs
    )

    # 🆕 Attach INDRA statement metadata
    if not df.empty and "curie" in df.columns:
        curie_to_statements = {}
        for curie in df["curie"].unique():
            kinase_id = curie.lower()
            stmt_list, _ = get_kinase_phosphosite_statements(
                kinase_id=kinase_id,
                phosphosites=[f"{gene}-{site}" for (gene, site) in phosphosite_ids],
                minimum_belief=minimum_belief,
                minimum_evidence=minimum_evidence_count,
                client=client
            )
            curie_to_statements[curie] = [
                {
                    "gene": agent.name if (agent := s.agent_list()[1]) else None,
                    "stmt_hash": s.get_hash(),
                    "belief": s.belief,
                    "evidence_count": len(s.evidence) if s.evidence else 0
                }
                for s in stmt_list
            ]
        df["statements"] = df["curie"].map(curie_to_statements)

    return df


def main():
    client = Neo4jClient()
    print("\nGO Enrichment\n")
    print(
        go_ora(client=client, gene_ids=EXAMPLE_GENE_IDS)
        .head(15)
        .to_markdown(index=False)
    )
    print("\n## WikiPathways Enrichment\n")
    print(
        wikipathways_ora(client=client, gene_ids=EXAMPLE_GENE_IDS)
        .head(15)
        .to_markdown(index=False)
    )
    print("\n## Reactome Enrichment\n")
    print(
        reactome_ora(client=client, gene_ids=EXAMPLE_GENE_IDS)
        .head(15)
        .to_markdown(index=False)
    )
    print("\n## Phenotype Enrichment\n")
    print(
        phenotype_ora(client=client, gene_ids=EXAMPLE_GENE_IDS)
        .head(15)
        .to_markdown(index=False)
    )
    print("\n## INDRA Upstream Enrichment\n")
    print(
        indra_upstream_ora(client=client, gene_ids=EXAMPLE_GENE_IDS)
        .head(15)
        .to_markdown(index=False)
    )
    print("\n## INDRA Downstream Enrichment\n")
    print(
        indra_downstream_ora(client=client, gene_ids=EXAMPLE_GENE_IDS)
        .head(15)
        .to_markdown(index=False)
    )


if __name__ == "__main__":
    main()
