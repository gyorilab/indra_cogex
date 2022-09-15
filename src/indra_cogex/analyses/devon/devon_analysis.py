""""""

import pandas as pd

from pathlib import Path
from protmapper import uniprot_client
from typing import Optional, Sequence, Iterable, Set
from indra.statements import Statement
from indra_cogex.client import queries, subnetwork
from indra_cogex.client import autoclient, Neo4jClient
import pystow
import pickle
from indra.assemblers.indranet import IndraNetAssembler

HERE = Path(__file__).parent.resolve()
PATH = HERE.joinpath("example_data.csv")
OUTPUT_MODULE = pystow.module("indra", "cogex", "analysis", "devon")
STATEMENTS_PKL_PATH = OUTPUT_MODULE.join(name="statements.pkl")
STATEMENTS_DF_PATH = OUTPUT_MODULE.join(name="statements.tsv")


def _hgnc_from_stmts(stmts: Iterable[Statement]) -> Set[str]:
    raise NotImplementedError


@autoclient()
def analysis(path: Path, target_hgnc_ids: set[str], *, client: Optional[Neo4jClient] = None):
    df = _read_df(path)
    measured = set(df[df.columns[0]])

    pairs = [("hgnc", gene_id) for gene_id in target_hgnc_ids]
    stmts = subnetwork.get_neighbor_network_statements(
        pairs, client=client, node_prefix="hgnc",
        minimum_evidence_count=10,
    )
    STATEMENTS_PKL_PATH.write_bytes(pickle.dumps(stmts))

    assembler = IndraNetAssembler(stmts)
    stmts_df = assembler.make_df()
    stmts_df.to_csv(STATEMENTS_DF_PATH, sep='\t', index=False)

    # neighbor_hgnc_ids = _hgnc_from_stmts(stmts)

    # overlap = measured.intersection(neighbor_hgnc_ids)
    # unmeasured = measured.difference(neighbor_hgnc_ids)


def _read_df(path):
    df = pd.read_csv(path, sep=",")
    index_column = df.columns[0]
    df[index_column] = df[index_column].map(uniprot_client.get_hgnc_id)
    df = df[df[index_column].notna()]
    return df


def main():
    #  RAS RAF MEK ERK
    query = [
        # RAS
        "6407",  # KRAS
        "5173",  # HRAS
        "7989",  # NRAS
        # RAF
        "1097",  # BRAF
        "646",  # ARAF
        "9829",  # RAF1
        # MEK
        "6840",  # MAP2K1
        "6842",  # MAP2K2
        # ERK
        "6871",
        "6877",
    ]
    analysis(path=PATH, target_hgnc_ids=query)


if __name__ == '__main__':
    main()
