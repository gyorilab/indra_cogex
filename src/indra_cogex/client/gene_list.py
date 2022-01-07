# -*- coding: utf-8 -*-

"""A collection of analyses possible on gene lists (of HGNC identifiers)."""

from collections import defaultdict
from functools import lru_cache
from textwrap import dedent
from typing import List, Mapping, Set, Tuple

import numpy as np
import pandas as pd
from scipy.stats import fisher_exact
from statsmodels.stats.multitest import multipletests

from indra_cogex.client.neo4j_client import Neo4jClient
from indra_cogex.client.queries import get_genes_for_go_term


def _prepare_hypergeometric_test(
    query_gene_set: Set[str],
    pathway_gene_set: Set[str],
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
    client: Neo4jClient, go_term: Tuple[str, str], gene_ids: List[str]
) -> Tuple[float, float]:
    """Get a a pair of odds ratio and p-value for the Fisher exact test a given GO term.

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
    return fisher_exact(table, alternative="greater")


@lru_cache(maxsize=1)
def _get_go(client: Neo4jClient) -> Mapping[str, Set[str]]:
    """Get a p-value for all GO terms.

    1. Look up genes associated with GO identifier
    2. Run ORA and return results
    """
    query = dedent(
        """\
        MATCH (term:BioEntity)-[:associated_with]-(gene:BioEntity)
        WHERE term.id STARTS WITH "go" and gene.id STARTS WITH "hgnc"
        RETURN term.id as go_curie, collect(gene.id) as gene_curies;
    """
    )
    curie_to_hgnc_ids = defaultdict(set)
    for result in client.query_tx(query):
        go_curie = result[0]
        hgnc_ids = {
            hgnc_curie.lower().removeprefix("hgnc:") for hgnc_curie in result[1]
        }
        curie_to_hgnc_ids[go_curie].update(hgnc_ids)
    return dict(curie_to_hgnc_ids)


def _do_ora(
    curie_to_hgnc_ids: Mapping[str, Set[str]], gene_ids: List[str], count: int
) -> pd.DataFrame:
    query_gene_set = set(gene_ids)
    rows = []
    for curie, pathway_hgnc_ids in curie_to_hgnc_ids.items():
        table = _prepare_hypergeometric_test(
            query_gene_set=query_gene_set,
            pathway_gene_set=pathway_hgnc_ids,
            gene_universe=count,
        )
        oddsratio, pvalue = fisher_exact(table, alternative="greater")
        rows.append((curie, oddsratio, pvalue))
    df = pd.DataFrame(rows, columns=["curie", "oddsratio", "p"])
    df["mlp"] = -np.log10(df["p"])
    correction_test = multipletests(df["p"], method="fdr_bh")
    df["q"] = correction_test[1]
    df["mlq"] = -np.log10(df["q"])
    df = df.sort_values("q", ascending=True)
    return df


def gene_ontology_ora(client: Neo4jClient, gene_ids: List[str]) -> pd.DataFrame:
    """Get a p-value for all GO terms.

    1. Look up genes associated with GO identifier
    2. Run ORA and return results
    """
    count = count_human_genes(client)
    curie_to_hgnc_ids = _get_go(client)
    return _do_ora(curie_to_hgnc_ids, gene_ids=gene_ids, count=count)


def main():
    client = Neo4jClient()
    go_term = ("GO", "GO:0000978")
    print(gene_ontology_ora(client, ["2916", "29147", "11197"]))


if __name__ == "__main__":
    main()
