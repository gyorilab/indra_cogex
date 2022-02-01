# -*- coding: utf-8 -*-

"""Weighted ORA."""

import pickle
from functools import lru_cache
from typing import Iterable, List, Mapping, Tuple

import numpy as np
import pandas as pd
import pystow
from scipy.stats import fisher_exact

from indra_cogex.client.enrichment.discrete import EXAMPLE_GENE_IDS, count_human_genes
from indra_cogex.client.neo4j_client import Neo4jClient

ENTITY_TO_TARGETS_CYPHER = """\
MATCH (regulator:BioEntity)-[r:indra_rel]->(gene:BioEntity)
WHERE
    gene.id STARTS WITH "hgnc"                  // Collecting human genes only
    AND r.stmt_type <> "Complex"                // Ignore complexes since they are non-directional
    AND NOT regulator.id STARTS WITH "uniprot"  // This is a simple way to ignore non-human proteins
RETURN 
    regulator.id,
    regulator.name,
    collect({gene:gene.id, evidence_count:r.evidence_count})
"""

TEST_RESULTS_PATH = pystow.join("indra", "cogex", name="weighted_ora_test_results.tsv")


@lru_cache(maxsize=1)
def _get_data(
    *,
    client: Neo4jClient,
    reload: bool = False,
    cutoff: int = 1,
) -> List[Tuple[str, str, Mapping[str, int]]]:
    cache_path = pystow.join(
        "indra", "cogex", name=f"weighted_ora_test_{cutoff:03d}.pkl"
    )
    if cache_path.exists() and not reload:
        with cache_path.open("rb") as file:
            return pickle.load(file)
    rv = [
        (
            curie,
            name,
            {
                collection_row["gene"]: collection_row["evidence_count"]
                for collection_row in collection_rows
                if cutoff <= collection_row["evidence_count"]
            },
        )
        for curie, name, collection_rows in client.query_tx(ENTITY_TO_TARGETS_CYPHER)
    ]
    with cache_path.open("wb") as file:
        pickle.dump(rv, file, protocol=pickle.HIGHEST_PROTOCOL)
    return rv


def indra_upstream_weighted_ora(
    gene_ids: Iterable[str],
    *,
    client: Neo4jClient,
    minimum_evidence_count: int = 1,
):
    gene_universe = count_human_genes(client=client)
    query_weights = {
        # TODO need some kind of pre-calculated global adjustment here
        gene_id: 1
        for gene_id in gene_ids
    }
    rows = []
    debug_rows = []
    for curie, name, pathway_weights in _get_data(client=client):
        # print(pathway_curie, pathway_name)
        # print(pathway_weights)
        # The weight for all remaining pathways is estimated by this.
        # Lots of room for improvemnt here. Maybe use label smoothing ideas?
        estimated_average_weight = sum(pathway_weights.values()) / gene_universe
        print(curie, estimated_average_weight)
        intersection = sum(
            pathway_weights[gene_id]
            for gene_id in set(query_weights).intersection(pathway_weights)
        )
        pathway_minus_query = sum(
            pathway_weights[gene_id]
            for gene_id in set(pathway_weights).difference(query_weights)
        )
        query_minus_pathway = sum(
            estimated_average_weight
            for _ in set(query_weights).difference(pathway_weights)
        )
        union = sum((intersection, pathway_minus_query, query_minus_pathway))
        total = gene_universe * estimated_average_weight
        bottom_right = total - union
        table = np.array(
            [
                [
                    intersection,
                    query_minus_pathway,
                ],
                [
                    pathway_minus_query,
                    bottom_right,
                ],
            ]
        )
        debug_rows.append(
            (
                curie,
                intersection,
                estimated_average_weight,
                query_minus_pathway,
                pathway_minus_query,
                union,
                total,
                bottom_right,
            )
        )
        _, pvalue = fisher_exact(table, alternative="greater")
        rows.append((curie, name, pvalue))

    df = pd.DataFrame(rows, columns=["curie", "name", "p"]).sort_values(
        "p", ascending=True
    )
    df["mlp"] = -np.log10(df["p"])
    return df


def indra_upstream_weighted_ora(
    gene_ids: Iterable[str],
    *,
    client: Neo4jClient,
    minimum_evidence_count: int = 1,
):
    gene_universe = count_human_genes(client=client)
    query_weights = {
        # TODO need some kind of pre-calculated global adjustment here
        gene_id: 1
        for gene_id in gene_ids
    }
    rows = []
    debug_rows = []
    for curie, name, pathway_weights in _get_data(client=client):
        estimated_average_weight = np.mean(
            np.fromiter(pathway_weights.values(), dtype=int)
        ).item()
        print(curie, estimated_average_weight)
        intersection = sum(
            pathway_weights[gene_id]
            for gene_id in set(query_weights).intersection(pathway_weights)
        )
        pathway_minus_query = sum(
            pathway_weights[gene_id]
            for gene_id in set(pathway_weights).difference(query_weights)
        )
        query_minus_pathway = sum(
            estimated_average_weight
            for _ in set(query_weights).difference(pathway_weights)
        )
        union = sum((intersection, pathway_minus_query, query_minus_pathway))
        total = gene_universe * estimated_average_weight
        bottom_right = total - union
        table = np.array(
            [
                [
                    intersection,
                    query_minus_pathway,
                ],
                [
                    pathway_minus_query,
                    bottom_right,
                ],
            ]
        )
        debug_rows.append(
            (
                curie,
                intersection,
                estimated_average_weight,
                query_minus_pathway,
                pathway_minus_query,
                union,
                total,
                bottom_right,
            )
        )
        _, pvalue = fisher_exact(table, alternative="greater")
        rows.append((curie, name, pvalue))

    df = pd.DataFrame(rows, columns=["curie", "name", "p"]).sort_values(
        "p", ascending=True
    )
    df["mlp"] = -np.log10(df["p"])
    return df


def _main():
    client = Neo4jClient()
    rv = indra_upstream_weighted_ora(gene_ids=EXAMPLE_GENE_IDS, client=client)
    rv.to_csv(TEST_RESULTS_PATH, sep="\t", index=False)
    print(TEST_RESULTS_PATH)


if __name__ == "__main__":
    _main()
