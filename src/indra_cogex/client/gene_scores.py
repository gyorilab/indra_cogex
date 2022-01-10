# -*- coding: utf-8 -*-

"""A collection of analyses possible on gene lists (of HGNC identifiers) with scores.

For example, this could be applied to the log_2 fold scores from differential gene
expression experiments.
"""

from pathlib import Path
from typing import Union

import gseapy
import pandas as pd
from gseapy.gsea import Prerank

import pyobo
from indra.databases import hgnc_client
from indra_cogex.client.gene_list import _get_wikipathways
from indra_cogex.client.neo4j_client import Neo4jClient

HERE = Path(__file__).parent.resolve()
RESULTS = HERE.joinpath("results")
RESULTS.mkdir(exist_ok=True)
TEST = HERE.joinpath("CS_ipsilateral_14D_vs_CS_contralateral_14D.csv")


def get_rat_scores(
    path: Union[Path, str],
    gene_symbol_col: str = "gene_name",
    log2fc_col: str = "log2FoldChange",
) -> dict[str, float]:
    """Load a differential gene expression file."""
    df = pd.read_csv(path)
    df["rgd_id"] = df[gene_symbol_col].map(pyobo.get_name_id_mapping("rgd"))
    df = df[df["rgd_id"].notna()]
    df["hgnc_id"] = df["rgd_id"].map(hgnc_client.get_hgnc_from_rat)
    df = df.set_index("hgnc_id")
    return df[log2fc_col].to_dict()


def gsea(
    scores: dict[str, float],
    gene_sets: dict[str, set[str]],
    directory: Union[Path, str],
    permutations: int = 100,
    seed: int = 6,
) -> Prerank:
    """Run GSEA on preranked data."""
    if isinstance(directory, str):
        directory = Path(directory)
    directory.mkdir(exist_ok=True, parents=True)
    return gseapy.prerank(
        rnk=pd.Series(scores),
        gene_sets=gene_sets,
        permutation_num=permutations,
        outdir=directory.as_posix(),
        format='svg',
        seed=seed,
    )


def main():
    """Run an example dataset from Sam."""
    client = Neo4jClient()
    gene_sets = {
        wikipathways_id: hgnc_gene_ids
        for (wikipathways_id, _), hgnc_gene_ids in _get_wikipathways(client).items()
    }
    scores = get_rat_scores(path=TEST)
    gsea(
        gene_sets=gene_sets,
        scores=scores,
        directory=RESULTS,
        permutations=100,
    )


if __name__ == "__main__":
    main()
