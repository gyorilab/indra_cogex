# -*- coding: utf-8 -*-

"""A collection of analyses possible on gene lists (of HGNC identifiers)."""

from collections import defaultdict
from functools import lru_cache
from textwrap import dedent
from typing import List, Mapping, Set

import numpy as np
import pandas as pd
from scipy.stats import fisher_exact
from statsmodels.stats.multitest import multipletests

from ..client.neo4j_client import Neo4jClient
from ..client.queries import get_genes_for_go_term


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
    query = dedent(f"""\
    MATCH (n:BioEntity {"db": "hgnc"})
    RETURN count(n) as count
    """)
    results = client.query_tx(query)
    return results[0][0]


def gene_ontology_single_ora(
    client: Neo4jClient, go_id: str, gene_ids: List[str]
) -> float:
    """Get a p-value for a given GO term.

    1. Look up genes associated with GO term
    2. Run ORA and return results
    """
    go_gene_ids = {
        gene.db_id
        for gene in get_genes_for_go_term(
            client=client, go_term=("GO", go_id), include_indirect=False
        )
    }
    count = count_human_genes(client)
    table = _prepare_hypergeometric_test(
        query_gene_set=set(gene_ids),
        pathway_gene_set=go_gene_ids,
        gene_universe=count,
    )
    _oddsratio, pvalue = fisher_exact(table, alternative="greater")
    return pvalue


@lru_cache(maxsize=1)
def _get_go(client: Neo4jClient) -> Mapping[str, Set[str]]:
    """Get a p-value for all GO terms.

    1. Look up genes associated with GO identifier
    2. Run ORA and return results
    """
    query = dedent("""\
        MATCH (term:BioEntity {})-[:associated_with]-(gene:BioEntity {})
        RETURN DISTINCT s
    """)
    go_term_to_genes = defaultdict(set)
    for result in client.query_tx(query):
        go_id = result[0]
        hgnc_id = result[1]
        go_term_to_genes[go_id].add(hgnc_id)
    return dict(go_term_to_genes)


def _do_ora(pathway_to_genes: Mapping[str, Set[str]], gene_ids: List[str], count: int) -> pd.DataFrame:
    query_gene_set = set(gene_ids)
    rows = []
    for go_id, go_gene_ids in pathway_to_genes.items():
        table = _prepare_hypergeometric_test(
            query_gene_set=query_gene_set,
            pathway_gene_set=go_gene_ids,
            gene_universe=count,
        )
        oddsratio, pvalue = fisher_exact(table, alternative="greater")
        rows.append((go_id, oddsratio, pvalue))
    df = pd.DataFrame(rows)
    correction_test = multipletests(df["p"], method="fdr_bh")
    df["q"] = correction_test[1]
    return df


def gene_ontology_ora(client: Neo4jClient, gene_ids: List[str]) -> pd.DataFrame:
    """Get a p-value for all GO terms.

    1. Look up genes associated with GO identifier
    2. Run ORA and return results
    """
    count = count_human_genes(client)
    go_term_to_genes = _get_go(client)
    return _do_ora(go_term_to_genes, gene_ids=gene_ids, count=count)
