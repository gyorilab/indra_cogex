import json
import logging
from collections import defaultdict
from functools import lru_cache
from typing import Optional, Tuple

import click
import numpy as np
import pandas as pd
import pystow
import seaborn as sns
from indra.belief import BeliefScorer, SimpleScorer
from indra.databases.mesh_client import is_disease
from indra.statements import stmt_from_json
from scipy.stats import binomtest
from statsmodels.stats.multitest import multipletests

from indra_cogex.client import Neo4jClient

__all__ = [
    "df_binomtest",
    "draw_p_comparsion",
    "_get_res",
]

logger = logging.getLogger(__name__)
MODULE = pystow.module("indra", "cogex", "analysis", "context_specificity")


def get_raw_counts_df(dd, c1, c2, minimum: Optional[int] = None):
    # 1. aggregate evidences in each publication by total count
    # 2. aggregate publications in each mesh annotation by sum of total counts
    raw_count_dd = {
        stmt_type: {
            (mesh_id, name): sum(len(evidences) for evidences in d2.values())
            for (mesh_id, name), d2 in d1.items()
        }
        for stmt_type, d1 in dd.items()
    }

    raw_count_df = pd.DataFrame(raw_count_dd).fillna(0).astype(int)

    if minimum is not None:
        # Apply cutoff for minimum number of annotations
        raw_count_df = raw_count_df[raw_count_df[c1] + raw_count_df[c2] > minimum]

    # Apply binomial test
    df_binomtest(raw_count_df, c1, c2)
    return raw_count_df


def get_norm_counts_df(dd, c1, c2, minimum: Optional[int] = None):
    # 1. aggregate evidences in each publication with a simple belief scorer
    # 2. aggregate publications in each mesh annotation by sum of total counts
    belief_scorer = SimpleScorer()
    belief_dd = {
        stmt_type: {
            (mesh_id, name): sum(
                belief_scorer.score_evidence_list(evidences)
                for evidences in d2.values()
            )
            for (mesh_id, name), d2 in d1.items()
        }
        for stmt_type, d1 in dd.items()
    }

    belief_df = pd.DataFrame(belief_dd).fillna(0.0).astype(float).round(2)

    if minimum is not None:
        # Apply cutoff for minimum number of normalized annotations
        belief_df = belief_df[belief_df[c1] + belief_df[c2] > minimum]

    # Apply binomial test
    belief_df, _ = df_binomtest(belief_df.round().astype(int), c1, c2)

    return belief_df


def df_binomtest(
    df: pd.DataFrame,
    c1: str,
    c2: str,
    sort: bool = True,
    sign: bool = True,
    method: str = "fdr_bh",
) -> Tuple[pd.DataFrame, float]:
    p = df[c1].sum() / (df[c1].sum() + df[c2].sum())
    logger.info("using calculated ratio of %.2f", p)

    # https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.binomtest.html
    df["p"] = [
        binomtest(
            k=activation_count,
            n=activation_count + inhibition_count,
            p=p,
        ).pvalue
        for activation_count, inhibition_count in df[[c1, c2]].values
    ]
    correction_results = multipletests(
        df["p"],
        method=method,
        is_sorted=False,
        alpha=0.05,
    )
    df["q"] = correction_results[1]
    # df["mlq"] = -np.log10(df["q"]).round(2)
    if sort:
        df.sort_values(["p"], inplace=True, ascending=True)

    if sign:
        df["sign"] = np.sign(df[c1] - (df[c1] + df[c2]) * p)
    return df, p


def draw_p_comparsion(
    raw_count_df, norm_count_df, p_cutoff: float = 0.05, c1=None, c2=None
):
    columns = ["p", "sign"]
    join_df = raw_count_df[columns].join(
        norm_count_df[columns], rsuffix="_norm", lsuffix="_direct"
    )
    ax = sns.scatterplot(data=join_df, x="p_direct", y="p_norm", alpha=0.8)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("Direct Count $p$-value")
    ax.set_ylabel("Normalized Count $p$-value")
    ax.invert_yaxis()
    ax.invert_xaxis()
    ax.axvline(p_cutoff, linestyle="--", color="red", alpha=0.8)
    ax.axhline(p_cutoff, linestyle="--", color="red", alpha=0.8)
    title = f"Summary of {len(join_df.index)} MeSH Annotations"
    if c1 and c2:
        title = f"{title}\nfor {c1} and {c2}"
    ax.set_title(title)


@lru_cache
def _get_res(client: Neo4jClient, source_curie: str, target_curie: str):
    return [
        (stmt_hash, ev, stmt_from_json(json.loads(stmt_json)), annotations)
        for stmt_hash, ev, stmt_json, annotations in client.query_tx(
            """\
            MATCH (:BioEntity {id: "%s"})-[r:indra_rel {stmt_type: 'Activation'}]->(:BioEntity {id: '%s'})
            MATCH (:Evidence {stmt_hash: r.stmt_hash})-[:has_citation]->(p:Publication)-[:annotated_with]->(a:BioEntity)
            RETURN r.stmt_hash, r.evidence_count, r.stmt_json, collect({publication: p.id, identifier: a.id, name: a.name})
            """
            % (source_curie, target_curie)
        )
    ]


def pipeline1(client: Neo4jClient):
    annotations_dd = defaultdict(lambda: defaultdict(set))
    source_curie = "hgnc:11892"
    for target_curie, target_name in [
        ("go:0006915", "apoptotic process"),
        ("go:0006954", "inflammatory response"),
        ## ('go:0008219', 'cell death'),
        # ('go:0008283', 'cell population proliferation'),
    ]:
        for _, _, stmt, annotations in _get_res(
            client=client,
            source_curie=source_curie,
            target_curie=target_curie,
        ):
            for annotation in annotations:
                mesh_id = annotation["identifier"].removeprefix("mesh:")
                if not is_disease(mesh_id):
                    continue
                annotations_dd[target_curie, target_name][
                    mesh_id, annotation["name"]
                ].add(annotation["publication"])

    df = (
        pd.DataFrame(
            {
                name: {
                    (mesh_id, mesh_name): len(ddata)
                    for (mesh_id, mesh_name), ddata in data.items()
                }
                for (_, name), data in annotations_dd.items()
            }
        )
        .fillna(0)
        .astype(int)
    )
    df.index.set_names(["mesh", "disease"], inplace=True)
    df.sort_values(df.columns[0], inplace=True, ascending=False)
    df = df[df.sum(axis=1) > 10]
    df_binomtest(df, "apoptotic process", "inflammatory response", sort=True)
    return df


def main():
    client = Neo4jClient()
    df1 = pipeline1(client)
    df1_path = MODULE.join(name="pipeline1_results.tsv")
    df1.to_csv(df1_path, sep="\t")
    click.echo(f"output to {df1_path}")


if __name__ == "__main__":
    main()
