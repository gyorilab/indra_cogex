# -*- coding: utf-8 -*-

"""Weighted ORA."""

import pickle
from typing import Iterable, Mapping, Optional, Set, Tuple

import numpy as np
import pandas as pd
import pystow
from scipy.stats import fisher_exact
from statsmodels.stats.multitest import multipletests
from tqdm.auto import tqdm

from .utils import get_wikipathways
from ..neo4j_client import Neo4jClient, autoclient

__all__ = [
    "get_weighted_contingency",
    "get_lookup",
    "get_gene_universe",
    # Pathway database functions
    "wikipathways_weighted_downstream_ora",
    "wikipathways_weighted_upstream_ora",
]

LOOKUP_CACHE_PATH = pystow.join("indra", name="weighted_ora_belief_cache.tsv")
LOOKUP_DICT_PATH = pystow.join("indra", name="weighted_ora_belief_cache.pkl")

ALL_BELIEFS_CYPHER = """\
MATCH (h:BioEntity)-[r:indra_rel]->(t:BioEntity)
WHERE
    h.id STARTS WITH "hgnc"                  // Collecting human genes only
    AND t.id STARTS WITH "hgnc"
    AND r.stmt_type <> "Complex"             // Ignore complexes since they are non-directional
RETURN DISTINCT
    h.id, t.id, r.stmt_hash, r.belief
"""

ALL_GENES_CYPHER = """\
MATCH (n:BioEntity)
WHERE n.id STARTS WITH 'hgnc'
RETURN n.id
"""


@autoclient(cache=True, maxsize=1)
def get_lookup(
    *, client: Neo4jClient, force: bool = False
) -> Mapping[Tuple[str, str], float]:
    """Get the source/target to belief lookup table."""
    if LOOKUP_DICT_PATH.is_file() and not force:
        return pickle.loads(LOOKUP_DICT_PATH.read_bytes())
    if LOOKUP_CACHE_PATH.is_file() and not force:
        df = pd.read_csv(LOOKUP_CACHE_PATH, sep="\t")
    else:
        res = client.query_tx(all_beliefs)
        df = pd.DataFrame(res, columns=["source", "target", "stmt_hash", "belief"])
        df.to_csv(LOOKUP_CACHE_PATH, sep="\t", index=False)
    rv = df.groupby(["source", "target"])["belief"].max().to_dict()
    LOOKUP_DICT_PATH.write_bytes(pickle.dumps(rv))
    return rv


@autoclient(cache=True, maxsize=1)
def get_gene_universe(*, client: Neo4jClient) -> Set[str]:
    return {row[0] for row in client.query_tx(ALL_GENES_CYPHER)}


def get_weighted_contingency(
    query_gene_set: Set[str],
    pathway_gene_set: Set[str],
    universe: Set[str],  # all gene CURIEs
    lookup: Mapping[Tuple[str, str], float],
    query_is_source: bool = True,
) -> np.ndarray:
    a_11, a_12, a_21, a_22 = 0.0, 0.0, 0.0, 0.0

    for gene in universe:
        # TODO could also use mean or median
        query_v = np.max(
            [
                lookup.get(
                    (query_gene, gene) if query_is_source else (gene, query_gene), 0.0
                )
                for query_gene in query_gene_set
            ]
        )
        m_query_v = 1.0 - query_v

        if gene in pathway_gene_set:
            pathway_v = 1.0
            m_pathway_v = 0.0
        else:
            pathway_v = 0.0
            m_pathway_v = 1.0

        a_11 += query_v * pathway_v
        a_12 += query_v * m_pathway_v
        a_21 += m_query_v * pathway_v
        a_22 += m_query_v * m_pathway_v

    return np.array([[a_11, a_12], [a_21, a_22]])


def _do_weighted_ora(
    *,
    curie_to_hgnc_ids: Mapping[Tuple[str, str], Set[str]],
    gene_ids: Iterable[str],
    universe: Set[str],
    lookup: Mapping[Tuple[str, str], float],
    method: Optional[str] = "fdr_bh",
    alpha: Optional[float] = None,
    keep_insignificant: bool = True,
    query_is_source: bool = True,
    use_tqdm: bool = True,
) -> pd.DataFrame:
    if alpha is None:
        alpha = 0.05
    query_gene_set = set(gene_ids)
    rows = []

    _tqdm_kwargs = dict(desc="Weighted ORA", unit="pathway", unit_scale=True)
    it = tqdm(curie_to_hgnc_ids.items(), disable=not use_tqdm, **_tqdm_kwargs)
    for (curie, name), pathway_hgnc_ids in it:
        table = get_weighted_contingency(
            query_gene_set=query_gene_set,
            pathway_gene_set=pathway_hgnc_ids,
            universe=universe,
            query_is_source=query_is_source,
            lookup=lookup,
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


def _ora(func, query_is_source, client: Neo4jClient, **kwargs):
    universe = get_gene_universe(client=client)
    lookup = get_lookup(client=client)
    return _do_weighted_ora(
        curie_to_hgnc_ids=func(client=client),
        query_is_source=query_is_source,
        universe=universe,
        lookup=lookup,
        **kwargs,
    )


@autoclient()
def wikipathways_weighted_upstream_ora(
    gene_ids: Iterable[str], *, client: Neo4jClient, **kwargs
) -> pd.DataFrame:
    """Calculate weighted over-representation on all WikiPathway pathways.

    Parameters
    ----------
    gene_ids :
        List of gene identifiers
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
    return _ora(
        func=get_wikipathways,
        client=client,
        query_is_source=True,
        gene_ids=gene_ids,
        **kwargs,
    )


@autoclient()
def wikipathways_weighted_downstream_ora(
    gene_ids: Iterable[str], *, client: Neo4jClient, **kwargs
) -> pd.DataFrame:
    """Calculate weighted over-representation on all WikiPathway pathways.

    Parameters
    ----------
    gene_ids :
        List of gene identifiers
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
    return _ora(
        func=get_wikipathways,
        client=client,
        query_is_source=False,
        gene_ids=gene_ids,
        **kwargs,
    )
