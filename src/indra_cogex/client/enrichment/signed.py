# -*- coding: utf-8 -*-

"""A collection of analyses possible on pairs of gene lists (of HGNC identifiers)."""

from pathlib import Path
from textwrap import dedent
from typing import Iterable, Optional

import pandas as pd
import pystow
import scipy.stats

from indra_cogex.client.enrichment.utils import collect_gene_sets
from indra_cogex.client.neo4j_client import Neo4jClient, autoclient

HERE = Path(__file__).parent.resolve()


# FIXME should we further limit this query to only a certain type of entities,
#  or split it up at least? (e.g., specific analysis for chemicals, genes, etc.)


def _query(
    stmt_types: Iterable[str],
    minimum_evidence_count: Optional[int] = None,
    minimum_belief: Optional[float] = None,
) -> str:
    """Return a query over INDRA relations f the given statement types."""
    query_range = ", ".join(f'"{stmt_type}"' for stmt_type in sorted(stmt_types))
    if minimum_evidence_count is None or minimum_evidence_count == 1:
        evidence_line = ""
    else:
        evidence_line = f"AND r.evidence_count >= {minimum_evidence_count}"
    if minimum_belief is None or minimum_belief == 0.0:
        belief_line = ""
    else:
        belief_line = f"AND r.belief >= {minimum_belief}"
    return dedent(
        f"""\
        MATCH (regulator:BioEntity)-[r:indra_rel]->(gene:BioEntity)
        WHERE gene.id STARTS WITH "hgnc"                // Collecting human genes only
            AND r.stmt_type in [{query_range}]          // Ignore complexes since they are non-directional
            AND NOT regulator.id STARTS WITH "uniprot"  // This is a simple way to ignore non-human proteins
            {evidence_line}
            {belief_line}
        RETURN regulator.id, regulator.name, collect(gene.id);
    """
    )


# TODO should this include other statement types? is the mechanism linker applied before
#  importing the database into CoGEx?

POSITIVE_STMTS = ["Activation", "IncreaseAmount"]
NEGATIVE_STMTS = ["Inhibition", "DecreaseAmount"]


@autoclient()
def reverse_causal_reasoning(
    positive_hgnc_ids: Iterable[str],
    negative_hgnc_ids: Iterable[str],
    minimum_size: int = 4,
    positive_stmts: Optional[Iterable[str]] = None,
    negative_stmts: Optional[Iterable[str]] = None,
    alpha: Optional[float] = None,
    keep_insignificant: bool = True,
    *,
    client: Neo4jClient,
    minimum_evidence_count: Optional[int] = None,
    minimum_belief: Optional[float] = None,
) -> pd.DataFrame:
    """Implement the Reverse Causal Reasoning algorithm from [catlett2013]_.

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
    positive_stmts :
        A list of statement types for identifying positive genes
    negative_stmts :
        A list of statement types for identifying negative genes
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

    .. [catlett2013] Catlett, N. L., *et al.* (2013). `Reverse causal reasoning: applying
       qualitative causal knowledge to the interpretation of high-throughput data
       <https://doi.org/10.1186/1471-2105-14-340>`_. BMC Bioinformatics, **14**(1), 340.
    """
    if alpha is None:
        alpha = 0.05
    positive_hgnc_ids = set(positive_hgnc_ids)
    negative_hgnc_ids = set(negative_hgnc_ids)
    database_positive = collect_gene_sets(
        query=_query(
            positive_stmts or POSITIVE_STMTS,
            minimum_evidence_count=minimum_evidence_count,
            minimum_belief=minimum_belief,
        ),
        client=client,
    )
    database_negative = collect_gene_sets(
        query=_query(
            negative_stmts or NEGATIVE_STMTS,
            minimum_evidence_count=minimum_evidence_count,
            minimum_belief=minimum_belief,
        ),
        client=client,
    )
    entities = set(database_positive).union(database_negative)

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
    if not keep_insignificant:
        df = df[df["binom_pvalue"] < alpha]
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
