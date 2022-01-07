# -*- coding: utf-8 -*-

"""A collection of analyses possible on gene lists (of HGNC identifiers)."""

from collections import defaultdict
from functools import lru_cache
from textwrap import dedent
from typing import Iterable, Optional

import numpy as np
import pandas as pd
from scipy.stats import fisher_exact
from statsmodels.stats.multitest import multipletests

from indra_cogex.client.neo4j_client import Neo4jClient
from indra_cogex.client.queries import get_genes_for_go_term

__all__ = [
    "go_ora",
    "wikipathways_ora",
    "reactome_ora",
    "indra_downstream_ora",
    "indra_upstream_ora",
]


def _prepare_hypergeometric_test(
    query_gene_set: set[str],
    pathway_gene_set: set[str],
    gene_universe: int,
) -> np.ndarray:
    """Prepare the matrix for hypergeometric test calculations.

    :param query_gene_set: gene set to test against pathway
    :param pathway_gene_set: pathway gene set
    :param gene_universe: number of HGNC symbols
    :return: 2x2 matrix
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
    client: Neo4jClient, go_term: tuple[str, str], gene_ids: list[str]
) -> float:
    """Get the p-value for the Fisher exact test a given GO term.

    1. Look up genes associated with GO term
    2. Run ORA and return results

    """
    count = count_human_genes(client)
    go_gene_ids = {
        gene.db_id
        for gene in get_genes_for_go_term(
            client=client, go_term=go_term, include_indirect=False
        )
    }
    table = _prepare_hypergeometric_test(
        query_gene_set=set(gene_ids),
        pathway_gene_set=go_gene_ids,
        gene_universe=count,
    )
    return fisher_exact(table, alternative="greater")[1]


@lru_cache(maxsize=1)
def _get_go(client: Neo4jClient) -> dict[tuple[str, str], set[str]]:
    """Get GO gene sets."""
    query = dedent(
        """\
        MATCH (term:BioEntity)-[:associated_with]-(gene:BioEntity)
        WHERE term.id STARTS WITH "go" and gene.id STARTS WITH "hgnc"
        RETURN term.id, term.name, collect(gene.id) as gene_curies;
    """
    )
    return _collect_pathways(client, query)


@lru_cache(maxsize=1)
def _get_wikipathways(client: Neo4jClient) -> dict[tuple[str, str], set[str]]:
    """Get WikiPathways gene sets."""
    query = dedent(
        """\
        MATCH (pathway:BioEntity)-[:haspart]-(gene:BioEntity)
        WHERE pathway.id STARTS WITH "wikipathways" and gene.id STARTS WITH "hgnc"
        RETURN pathway.id, pathway.name, collect(gene.id);
    """
    )
    return _collect_pathways(client, query)


@lru_cache(maxsize=1)
def _get_reactome(client: Neo4jClient) -> dict[tuple[str, str], set[str]]:
    """Get Reactome gene sets."""
    query = dedent(
        """\
        MATCH (pathway:BioEntity)-[:haspart]-(gene:BioEntity)
        WHERE pathway.id STARTS WITH "reactome" and gene.id STARTS WITH "hgnc"
        RETURN pathway.id, pathway.name, collect(gene.id);
    """
    )
    return _collect_pathways(client, query)


@lru_cache(maxsize=1)
def _get_indra_downstream(client: Neo4jClient) -> dict[tuple[str, str], set[str]]:
    """Get gene sets for each entity in INDRA based on the genes that it regulates/has statements to."""
    query = dedent(
        """\
        MATCH (regulator:BioEntity)-[:indra_rel]->(gene:BioEntity)
        WHERE gene.id STARTS WITH "hgnc"
        RETURN regulator.id, regulator.name, collect(gene.id);
    """
    )
    return _collect_pathways(client, query)


@lru_cache(maxsize=1)
def _get_indra_upstream(client: Neo4jClient) -> dict[tuple[str, str], set[str]]:
    """Get gene sets for each entity in INDRA based on what entities regulate it."""
    query = dedent(
        """\
        MATCH (gene:BioEntity)-[:indra_rel]->(regulator:BioEntity)
        WHERE gene.id STARTS WITH "hgnc"
        RETURN regulator.id, regulator.name, collect(gene.id);
    """
    )
    return _collect_pathways(client, query)


def _collect_pathways(
    client: Neo4jClient, query: str
) -> dict[tuple[str, str], set[str]]:
    curie_to_hgnc_ids = defaultdict(set)
    for result in client.query_tx(query):
        curie = result[0]
        name = result[1]
        hgnc_ids = {
            hgnc_curie.lower().removeprefix("hgnc:") for hgnc_curie in result[2]
        }
        curie_to_hgnc_ids[curie, name].update(hgnc_ids)
    return dict(curie_to_hgnc_ids)


def _do_ora(
    curie_to_hgnc_ids: dict[tuple[str, str], set[str]],
    gene_ids: Iterable[str],
    count: int,
    method: Optional[str] = "fdr_bh",
    alpha: Optional[float] = None,
) -> pd.DataFrame:
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
            alpha=alpha or 0.05,
        )
        df["q"] = correction_results[1]
        df["mlq"] = -np.log10(df["q"])
        df = df.sort_values("q", ascending=True)
    return df


def go_ora(client: Neo4jClient, gene_ids: Iterable[str], **kwargs) -> pd.DataFrame:
    """Calculate over-representation on all GO terms."""
    count = count_human_genes(client)
    return _do_ora(_get_go(client), gene_ids=gene_ids, count=count, **kwargs)


def wikipathways_ora(
    client: Neo4jClient, gene_ids: Iterable[str], **kwargs
) -> pd.DataFrame:
    """Calculate over-representation on all WikiPathway pathways."""
    count = count_human_genes(client)
    return _do_ora(_get_wikipathways(client), gene_ids=gene_ids, count=count, **kwargs)


def reactome_ora(
    client: Neo4jClient, gene_ids: Iterable[str], **kwargs
) -> pd.DataFrame:
    """Calculate over-representation on all Reactome pathways."""
    count = count_human_genes(client)
    return _do_ora(_get_reactome(client), gene_ids=gene_ids, count=count, **kwargs)


def indra_upstream_ora(
    client: Neo4jClient, gene_ids: Iterable[str], **kwargs
) -> pd.DataFrame:
    """Calculate over-representation on INDRA in-edges."""
    count = count_human_genes(client)
    return _do_ora(
        _get_indra_upstream(client), gene_ids=gene_ids, count=count, **kwargs
    )


def indra_downstream_ora(
    client: Neo4jClient, gene_ids: Iterable[str], **kwargs
) -> pd.DataFrame:
    """Calculate over-representation on INDRA out-edges."""
    count = count_human_genes(client)
    return _do_ora(
        _get_indra_downstream(client), gene_ids=gene_ids, count=count, **kwargs
    )


def main():
    client = Neo4jClient()

    # fmt: off
    #: This example list comes from human genes associated with COVID-19 (https://bgee.org/?page=top_anat#/result/9bbddda9dea22c21edcada56ad552a35cb8e29a7/)
    example_gene_ids = [
        "613", "1116", "1119", "1697", "7067", "2537", "2734", "29517", "8568", "4910", "4931", "4932", "4962", "4983",
        "18873", "5432", "5433", "5981", "16404", "5985", "18358", "6018", "6019", "6021", "6118", "6120", "6122",
        "6148", "6374", "6378", "6395", "6727", "14374", "8004", "18669", "8912", "30306", "23785", "9253", "9788",
        "10498", "10819", "6769", "11120", "11133", "11432", "11584", "18348", "11849", "28948", "11876", "11878",
        "11985", "20820", "12647", "20593", "12713"
    ]
    # fmt: on

    print("\nGO Enrichment\n")
    print(go_ora(client, example_gene_ids).head(15).to_markdown(index=False))
    print("\n## WikiPathways Enrichment\n")
    print(wikipathways_ora(client, example_gene_ids).head(15).to_markdown(index=False))
    print("\n## Reactome Enrichment\n")
    print(reactome_ora(client, example_gene_ids).head(15).to_markdown(index=False))
    print("\n## INDRA Upstream Enrichment\n")
    print(
        indra_upstream_ora(client, example_gene_ids).head(15).to_markdown(index=False)
    )
    print("\n## INDRA Downstream Enrichment\n")
    print(
        indra_downstream_ora(client, example_gene_ids).head(15).to_markdown(index=False)
    )


if __name__ == "__main__":
    main()
