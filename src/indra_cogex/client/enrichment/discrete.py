# -*- coding: utf-8 -*-

"""A collection of analyses possible on gene lists (of HGNC identifiers)."""

from functools import lru_cache
from typing import Dict, Iterable, List, Optional, Set, Tuple

import numpy as np
import pandas as pd
from scipy.stats import fisher_exact
from statsmodels.stats.multitest import multipletests

from indra_cogex.client.enrichment.utils import (
    get_entity_to_regulators,
    get_entity_to_targets,
    get_go,
    get_reactome,
    get_wikipathways,
)
from indra_cogex.client.neo4j_client import Neo4jClient
from indra_cogex.client.queries import get_genes_for_go_term

__all__ = [
    "go_ora",
    "wikipathways_ora",
    "reactome_ora",
    "indra_downstream_ora",
    "indra_upstream_ora",
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
    query_gene_set: Set[str],
    pathway_gene_set: Set[str],
    gene_universe: int,
) -> np.ndarray:
    """Prepare the matrix for hypergeometric test calculations.

    Parameters
    ----------
    query_gene_set:
        gene set to test against pathway
    pathway_gene_set:
        pathway gene set
    gene_universe:
        number of HGNC symbols

    Returns
    -------
    :
        A 2x2 matrix
    """
    return np.array(
        [
            [
                len(query_gene_set.intersection(pathway_gene_set)),
                len(query_gene_set.difference(pathway_gene_set)),
            ],
            [
                len(pathway_gene_set.difference(query_gene_set)),
                gene_universe - len(pathway_gene_set.union(query_gene_set)),
            ],
        ]
    )


@lru_cache(maxsize=1)
def count_human_genes(client: Neo4jClient) -> int:
    """Count the number of HGNC genes in neo4j."""
    query = f"""\
        MATCH (n:BioEntity)
        WHERE n.id STARTS WITH 'hgnc'
        RETURN count(n) as count
    """
    results = client.query_tx(query)
    return results[0][0]


def gene_ontology_single_ora(
    client: Neo4jClient, go_term: Tuple[str, str], gene_ids: List[str]
) -> float:
    """Get the *p*-value for the Fisher exact test a given GO term.

    1. Look up genes associated with GO term or child terms
    2. Run ORA and return results
    """
    count = count_human_genes(client=client)
    go_gene_ids = {
        gene.db_id
        for gene in get_genes_for_go_term(
            client=client, go_term=go_term, include_indirect=True
        )
    }
    table = _prepare_hypergeometric_test(
        query_gene_set=set(gene_ids),
        pathway_gene_set=go_gene_ids,
        gene_universe=count,
    )
    return fisher_exact(table, alternative="greater")[1]


def _do_ora(
    curie_to_hgnc_ids: Dict[Tuple[str, str], Set[str]],
    gene_ids: Iterable[str],
    count: int,
    method: Optional[str] = "fdr_bh",
    alpha: Optional[float] = None,
    keep_insignificant: bool = True,
) -> pd.DataFrame:
    if alpha is None:
        alpha = 0.05
    query_gene_set = set(gene_ids)
    rows = []
    for (curie, name), pathway_hgnc_ids in curie_to_hgnc_ids.items():
        table = _prepare_hypergeometric_test(
            query_gene_set=query_gene_set,
            pathway_gene_set=pathway_hgnc_ids,
            gene_universe=count,
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


def go_ora(client: Neo4jClient, gene_ids: Iterable[str], **kwargs) -> pd.DataFrame:
    """Calculate over-representation on all GO terms."""
    count = count_human_genes(client=client)
    return _do_ora(get_go(client=client), gene_ids=gene_ids, count=count, **kwargs)


def wikipathways_ora(
    client: Neo4jClient, gene_ids: Iterable[str], **kwargs
) -> pd.DataFrame:
    """Calculate over-representation on all WikiPathway pathways."""
    count = count_human_genes(client=client)
    return _do_ora(
        get_wikipathways(client=client), gene_ids=gene_ids, count=count, **kwargs
    )


def reactome_ora(
    client: Neo4jClient, gene_ids: Iterable[str], **kwargs
) -> pd.DataFrame:
    """Calculate over-representation on all Reactome pathways."""
    count = count_human_genes(client=client)
    return _do_ora(
        get_reactome(client=client), gene_ids=gene_ids, count=count, **kwargs
    )


def indra_downstream_ora(
    client: Neo4jClient, gene_ids: Iterable[str], **kwargs
) -> pd.DataFrame:
    """
    Calculate a p-value for each entity in the INDRA database
    based on the genes that are causally upstream of it and how
    they compare to the query gene set.
    """
    count = count_human_genes(client=client)
    return _do_ora(
        get_entity_to_regulators(client=client),
        gene_ids=gene_ids,
        count=count,
        **kwargs,
    )


def indra_upstream_ora(
    client: Neo4jClient, gene_ids: Iterable[str], **kwargs
) -> pd.DataFrame:
    """
    Calculate a p-value for each entity in the INDRA database
    based on the set of genes that it regulates and how
    they compare to the query gene set.
    """
    count = count_human_genes(client=client)
    return _do_ora(
        get_entity_to_targets(client=client), gene_ids=gene_ids, count=count, **kwargs
    )


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
