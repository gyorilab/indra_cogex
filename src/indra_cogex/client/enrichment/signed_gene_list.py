# -*- coding: utf-8 -*-

"""A collection of analyses possible on pairs of gene lists (of HGNC identifiers)."""

from pathlib import Path
from textwrap import dedent

import pandas as pd
import pystow
import scipy.stats

from indra_cogex.client.neo4j_client import Neo4jClient

from .gene_list import _collect_pathways

HERE = Path(__file__).parent.resolve()


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


UP_STMTS = ["Activation", "IncreaseAmount"]
DOWN_STMTS = ["Inhibition", "DecreaseAmount"]


def reverse_causal_reasoning(
    client: Neo4jClient, up: set[str], down: set[str], minimum_size: int = 4
) -> pd.DataFrame:
    """Implement the Reverse Causal Reasoning algorithm from [catlett2013]_.

    :param client: A neo4j client
    :param up: A list of positive-signed HGNC gene identifiers
        (e.g., up-regulated genes in a differential gene expression analysis)
    :param down: A list of negative-signed HGNC gene identifiers
        (e.g., down-regulated genes in a differential gene expression analysis)
    :param minimum_size: The minimum number of entities marked as downstream
        of an entity for it to be usable as a hyp
    :returns: A pandas DataFrame with results for each entity in the graph
        database

    .. [catlett2013] Catlett, N. L., *et al* (2013). `Reverse causal reasoning: applying
       qualitative causal knowledge to the interpretation of high-throughput data
       <https://doi.org/10.1186/1471-2105-14-340>`_. BMC Bioinformatics, **14**(1), 340.
    """
    database_up = _collect_pathways(client, _query(UP_STMTS))
    database_down = _collect_pathways(client, _query(DOWN_STMTS))
    entities = set(database_up).union(database_down)

    rows = []
    for entity in entities:
        entity_up: set[str] = database_up.get(entity, set())
        entity_down: set[str] = database_down.get(entity, set())
        if len(entity_up) + len(entity_down) < minimum_size:
            continue  # skip this hypothesis
        correct, incorrect, ambiguous = 0, 0, 0
        for hgnc_id in up:
            if hgnc_id in entity_up and hgnc_id in entity_down:
                ambiguous += 1
            elif hgnc_id in entity_up:
                correct += 1
            elif hgnc_id in entity_down:
                incorrect += 1
            else:
                ambiguous += 1
        for hgnc_id in down:
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


# Examples taken as top 20 up and down
# genes from dz:135 in CREEDS (prostate cancer)
# fmt: off
EXAMPLE_UP_SYMBOLS = [
    "RPL41", "GAPDH", "CD63", "TGFBI", "HLA-B", "VIM", "LGALS1", "FTL",
    "TUBA1C", "RPL23A", "IGFBP3", "RPLP1" "UBC", "ACTB", "SPON2", "COL1A2",
    "RPL13A", "RPS10", "MT2A", "RPS18", "RPS15", "RPS13", "MEST", "RPS23",
    "HLA-C", "RPL30", "GSTO1", "DCN", "RPL32", "CCL11", "EEF1A1", "ALDH1A1",
    "TMSB10", "PENK", "RPLP0",
]
EXAMPLE_DOWN_SYMBOLS = [
    "IGFBP2", "TFRC", "COL15A1", "GREM1", "SMPDL3A", "FSTL1", "RPL19",
    "PABPC3", "RPS2", "MFAP4", "MMP2", "RLIM", "CEMIP", "LGALS3BP", "RPSA",
    "DYNLL1", "LXN", "TUBA1A", "EEF2", "NGFRAP1", "FTH1P5", "MXRA7", "RPS27A",
    "HIF1A", "CTSB", "RHOA", "RPL26", "LOC728825", "SERPINH1", "LAMC1",
    "A2M", "ACTN1", "EIF4A2", "FMOD", "TXNRD1",
]


# fmt: on


def main():
    """Demonstrate signed gene list functions."""
    from indra.databases import hgnc_client

    up = {
        hgnc_id
        for symbol in EXAMPLE_UP_SYMBOLS
        if (hgnc_id := hgnc_client.get_current_hgnc_id(symbol)) is not None
    }
    down = {
        hgnc_id
        for symbol in EXAMPLE_DOWN_SYMBOLS
        if (hgnc_id := hgnc_client.get_current_hgnc_id(symbol)) is not None
    }
    client = Neo4jClient()
    df = reverse_causal_reasoning(client, up=up, down=down)
    path = pystow.join("indra", "cogex", "demos", name="rcr_test.tsv")
    df.to_csv(path, sep="\t", index=False)


if __name__ == "__main__":
    main()
