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
STATEMENTS_DF_PATH = OUTPUT_MODULE.join(name="indranet.tsv")
PROCESSED_PATH = OUTPUT_MODULE.join(name="example_data_processed.csv")



def _hgnc_from_stmts(stmts: Iterable[Statement]) -> Set[str]:
    raise NotImplementedError


@autoclient()
def analysis(path: Path, target_hgnc_ids: set[str], *, client: Optional[Neo4jClient] = None):
    df = _read_df(path)
    measured = set(df["hgnc"])

    if not STATEMENTS_PKL_PATH.is_file():
        pairs = [("hgnc", gene_id) for gene_id in target_hgnc_ids]
        stmts = subnetwork.get_neighbor_network_statements(
            pairs, client=client, node_prefix="hgnc",
            minimum_evidence_count=8,
        )
        STATEMENTS_PKL_PATH.write_bytes(pickle.dumps(stmts))
    else:
        stmts = pickle.loads(STATEMENTS_PKL_PATH.read_bytes())

    assembler = IndraNetAssembler(stmts)
    stmts_df = assembler.make_df(keep_self_loops=False).sort_values(["agA_name", "agB_name"])
    stmts_df = stmts_df[stmts_df["stmt_type"] != "Complex"]
    stmts_df.drop_duplicates(subset=["stmt_hash"], inplace=True)
    stmts_df.to_csv(STATEMENTS_DF_PATH, sep='\t', index=False)

    # This doesn't print stuff that makes sense
    # assembler = SifAssembler(stmts)
    # assembler.make_model()
    # assembler.save_model(STATEMENTS_SIF_DF_PATH)

    neighbor_hgnc_ids = set(stmts_df["agA_id"]).union(stmts_df["agB_id"])
    df["in_neighbors"] = df["hgnc"].map(neighbor_hgnc_ids.__contains__)
    df.to_csv(PROCESSED_PATH, sep="\t", index=False)


def _read_df(path):
    df = pd.read_csv(path, sep=",")
    initial_columns = list(df.columns)
    df["hgnc"] = df[initial_columns[0]].map(uniprot_client.get_hgnc_id)
    df = df[df["hgnc"].notna()]
    df = df[["hgnc", *initial_columns]]
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
