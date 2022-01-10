# -*- coding: utf-8 -*-

"""A collection of analyses possible on pairs of gene lists (of HGNC identifiers)."""

from pathlib import Path
from textwrap import dedent
from typing import Iterable

import pandas as pd
import pystow
import scipy.stats

from indra_cogex.client.enrichment.utils import collect_gene_sets
from indra_cogex.client.neo4j_client import Neo4jClient

HERE = Path(__file__).parent.resolve()


# FIXME should we further limit this query to only a certain type of entities,
#  or split it up at least? (e.g., specific analysis for chemicals, genes, etc.)


def _query(stmt_types: list[str]) -> str:
    """Return a query over INDRA relations f the given statement types."""
    query_range = ", ".join(f'"{stmt_type}"' for stmt_type in sorted(stmt_types))
    return dedent(
        f"""\
        MATCH (regulator:BioEntity)-[r:indra_rel]->(gene:BioEntity)
        // Collecting human genes only
        WHERE gene.id STARTS WITH "hgnc"
        // Ignore complexes since they are non-directional
        AND r.stmt_type in [{query_range}]
        // This is a simple way to ignore non-human proteins
        AND NOT regulator.id STARTS WITH "uniprot"
        RETURN regulator.id, regulator.name, collect(gene.id);
    """
    )


# TODO should this include other statement types? is the mechanism linker applied before
#  importing the database into CoGEx?

UP_STMTS = ["Activation", "IncreaseAmount"]
DOWN_STMTS = ["Inhibition", "DecreaseAmount"]


def reverse_causal_reasoning(
    client: Neo4jClient,
    positive_hgnc_ids: Iterable[str],
    negative_hgnc_ids: Iterable[str],
    minimum_size: int = 4,
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

    Returns
    -------
    :
        A pandas DataFrame with results for each entity in the graph database

    .. [catlett2013] Catlett, N. L., *et al* (2013). `Reverse causal reasoning: applying
       qualitative causal knowledge to the interpretation of high-throughput data
       <https://doi.org/10.1186/1471-2105-14-340>`_. BMC Bioinformatics, **14**(1), 340.
    """
    positive_hgnc_ids = set(positive_hgnc_ids)
    negative_hgnc_ids = set(negative_hgnc_ids)
    database_up = collect_gene_sets(client, _query(UP_STMTS))
    database_down = collect_gene_sets(client, _query(DOWN_STMTS))
    entities = set(database_up).union(database_down)

    rows = []
    for entity in entities:
        entity_up: set[str] = database_up.get(entity, set())
        entity_down: set[str] = database_down.get(entity, set())
        if len(entity_up) + len(entity_down) < minimum_size:
            continue  # skip this hypothesis
        correct, incorrect, ambiguous = 0, 0, 0
        for hgnc_id in positive_hgnc_ids:
            if hgnc_id in entity_up and hgnc_id in entity_down:
                ambiguous += 1
            elif hgnc_id in entity_up:
                correct += 1
            elif hgnc_id in entity_down:
                incorrect += 1
            else:
                ambiguous += 1
        for hgnc_id in negative_hgnc_ids:
            if hgnc_id in entity_up and hgnc_id in entity_down:
                ambiguous += 1
            elif hgnc_id in entity_up:
                incorrect += 1
            elif hgnc_id in entity_down:
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
    return df


# Examples taken as top 40 up and down
# genes from dz:135 in CREEDS (prostate cancer)
# fmt: off
EXAMPLE_UP_HGNC_IDS = [
    "10354", "4141", "1692", "11771", "4932", "12692", "6561", "3999",
    "20768", "10317", "5472", "10372", "12468", "132", "11253", "2198",
    "10304", "10383", "7406", "10401", "10388", "10386", "7028", "10410",
    "4933", "10333", "13312", "2705", "10336", "10610", "3189", "402",
    "11879", "8831", "10371", "2528", "17194", "12458", "11553", "11820",
]
EXAMPLE_DOWN_HGNC_IDS = [
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
        client,
        positive_hgnc_ids=EXAMPLE_UP_HGNC_IDS,
        negative_hgnc_ids=EXAMPLE_DOWN_HGNC_IDS,
    )
    path = pystow.join("indra", "cogex", "demos", name="rcr_test.tsv")
    df.to_csv(path, sep="\t", index=False)


if __name__ == "__main__":
    main()
