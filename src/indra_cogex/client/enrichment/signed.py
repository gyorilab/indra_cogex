# -*- coding: utf-8 -*-

"""A collection of analyses possible on pairs of gene lists (of HGNC identifiers)."""

from pathlib import Path
from textwrap import dedent
from typing import Iterable, Optional

import pandas as pd
import pystow
import scipy.stats

from indra_cogex.client.enrichment.utils import (
    get_negative_stmt_sets,
    get_positive_stmt_sets,
)
from indra_cogex.client.neo4j_client import Neo4jClient, autoclient

HERE = Path(__file__).parent.resolve()


@autoclient()
def reverse_causal_reasoning(
    positive_hgnc_ids: Iterable[str],
    negative_hgnc_ids: Iterable[str],
    minimum_size: int = 4,
    alpha: Optional[float] = None,
    keep_insignificant: bool = True,
    *,
    client: Neo4jClient,
    minimum_evidence_count: Optional[int] = None,
    minimum_belief: Optional[float] = None,
) -> pd.DataFrame:
    """Implement the Reverse Causal Reasoning algorithm from
    :ref:`Catlett, N. L., et al. (2013) <ref-causal-reas-references>`.

    Parameters
    ----------
    client :
        A neo4j client
    positive_hgnc_ids :
        A list of positive-signed HGNC gene identifiers
        (e.g., up-regulated genes in a differential gene expression analysis)
    negative_hgnc_ids :
        A list of negative-signed HGNC gene identifiers
        (e.g., down-regulated genes in a differential gene expression analysis)
    minimum_size :
        The minimum number of entities marked as downstream
        of an entity for it to be usable as a hyp
    alpha :
        The cutoff for significance. Defaults to 0.05
    keep_insignificant :
        If false, removes results with a p value less than alpha.
    minimum_evidence_count :
        The minimum number of evidences for a relationship to count it as a regulator.
        Defaults to 1 (i.e., cutoff not applied).
    minimum_belief :
        The minimum belief for a relationship to count it as a regulator.
        Defaults to 0.0 (i.e., cutoff not applied).

    Returns
    -------
    :
        A pandas DataFrame with results for each entity in the graph database


    .. _ref-causal-reas-references:

    References
    ----------
    Catlett, N. L., *et al.* (2013): `Reverse causal reasoning: applying qualitative
    causal knowledge to the interpretation of high-throughput data
    <https://doi.org/10.1186/1471-2105-14-340>`_. BMC Bioinformatics, **14** (1), 340.
    """
    print(
        f"Starting reverse causal reasoning with {len(list(positive_hgnc_ids))} positive genes and {len(list(negative_hgnc_ids))} negative genes")
    print(f"Positive HGNC IDs: {list(positive_hgnc_ids)}")
    print(f"Negative HGNC IDs: {list(negative_hgnc_ids)}")
    print(f"Parameters: minimum_size={minimum_size}, alpha={alpha}, keep_insignificant={keep_insignificant}")
    print(f"Minimum evidence count: {minimum_evidence_count}, Minimum belief: {minimum_belief}")

    if alpha is None:
        alpha = 0.05
    positive_hgnc_ids = set(positive_hgnc_ids)
    negative_hgnc_ids = set(negative_hgnc_ids)

    print("Getting positive statement sets...")
    database_positive = get_positive_stmt_sets(
        client=client,
        minimum_belief=minimum_belief,
        minimum_evidence_count=minimum_evidence_count,
    )
    print(f"Number of entities with positive statements: {len(database_positive)}")

    print("Getting negative statement sets...")
    database_negative = get_negative_stmt_sets(
        client=client,
        minimum_belief=minimum_belief,
        minimum_evidence_count=minimum_evidence_count,
    )
    print(f"Number of entities with negative statements: {len(database_negative)}")

    entities = set(database_positive).union(database_negative)
    print(f"Total number of entities: {len(entities)}")

    rows = []
    for entity in entities:
        entity_positive: set[str] = database_positive.get(entity, set())
        entity_negative: set[str] = database_negative.get(entity, set())
        if len(entity_positive) + len(entity_negative) < minimum_size:
            continue  # skip this hypothesis

        correct, incorrect, ambiguous = 0, 0, 0
        for hgnc_id in positive_hgnc_ids:
            if hgnc_id in entity_positive and hgnc_id in entity_negative:
                ambiguous += 1
            elif hgnc_id in entity_positive:
                correct += 1
            elif hgnc_id in entity_negative:
                incorrect += 1
            else:
                ambiguous += 1
        for hgnc_id in negative_hgnc_ids:
            if hgnc_id in entity_positive and hgnc_id in entity_negative:
                ambiguous += 1
            elif hgnc_id in entity_positive:
                incorrect += 1
            elif hgnc_id in entity_negative:
                correct += 1
            else:
                ambiguous += 1

        if correct + incorrect:
            res = scipy.stats.binomtest(
                correct, correct + incorrect, alternative="greater"
            )
            res_p = res.pvalue
            res_ambig = scipy.stats.binomtest(
                correct, correct + incorrect + ambiguous, alternative="greater"
            )
            res_ambig_p = res_ambig.pvalue
        else:
            res_p, res_ambig_p = None, None
        rows.append((*entity, correct, incorrect, ambiguous, res_p, res_ambig_p))

    print(f"Number of rows before DataFrame creation: {len(rows)}")
    df = pd.DataFrame(
        rows,
        columns=[
            "curie",
            "name",
            "correct",
            "incorrect",
            "ambiguous",
            "binom_pvalue",
            "binom_ambig_pvalue",
        ],
    ).sort_values("binom_pvalue")
    print(f"DataFrame shape after creation: {df.shape}")

    if not keep_insignificant:
        df = df[df["binom_pvalue"] < alpha]
        print(f"DataFrame shape after removing insignificant results: {df.shape}")

    print(f"Final DataFrame shape: {df.shape}")
    print(f"Final DataFrame head:\n{df.head()}")

    return df


# Examples taken as top 40 up and down
# genes from dz:135 in CREEDS (prostate cancer)
# fmt: off
EXAMPLE_POSITIVE_HGNC_IDS = [
    "10354", "4141", "1692", "11771", "4932", "12692", "6561", "3999",
    "20768", "10317", "5472", "10372", "12468", "132", "11253", "2198",
    "10304", "10383", "7406", "10401", "10388", "10386", "7028", "10410",
    "4933", "10333", "13312", "2705", "10336", "10610", "3189", "402",
    "11879", "8831", "10371", "2528", "17194", "12458", "11553", "11820",
]
EXAMPLE_NEGATIVE_HGNC_IDS = [
    "5471", "11763", "2192", "2001", "17389", "3972", "10312", "8556",
    "10404", "7035", "7166", "13429", "29213", "6564", "6502", "15476",
    "13347", "20766", "3214", "13388", "3996", "7541", "10417", "4910",
    "2527", "667", "10327", "1546", "6492", "7", "163", "3284", "3774",
    "12437", "8547", "6908", "3218", "10424", "10496", "1595",
]


# fmt: on


def main():
    """Demonstrate signed gene list functions."""
    client = Neo4jClient()
    df = reverse_causal_reasoning(
        client=client,
        positive_hgnc_ids=EXAMPLE_POSITIVE_HGNC_IDS,
        negative_hgnc_ids=EXAMPLE_NEGATIVE_HGNC_IDS,
    )
    path = pystow.join("indra", "cogex", "demos", name="rcr_test.tsv")
    df.to_csv(path, sep="\t", index=False)


if __name__ == "__main__":
    main()
