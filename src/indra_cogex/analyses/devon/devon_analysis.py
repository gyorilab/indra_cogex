""""""

import bioregistry
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


def get_query(fname: str) -> Set[str]:
    """Get the query HGNC identifier set from one of the files in this directory by name."""
    prefix, *lines = HERE.joinpath(fname).read_text().splitlines()
    lines = {line.strip() for line in lines if line.strip()}
    norm_prefix = bioregistry.normalize_prefix(prefix)
    if norm_prefix is None:
        raise ValueError(f"invalid prefix: {prefix}")
    if norm_prefix == "hgnc":
        return lines
    elif norm_prefix == "uniprot":
        return {
            uniprot_client.get_hgnc_id(line)
            for line in lines
        }
    else:
        raise ValueError(f"unhandled prefix: {norm_prefix}")


@autoclient()
def analysis_hgnc(
    data_path: Path,
    target_hgnc_ids: Iterable[str], *,
    analysis_id: str,
    client: Optional[Neo4jClient] = None,
):
    analysis_module = OUTPUT_MODULE.module(analysis_id)
    statements_pkl_path = analysis_module.join(name="statements.pkl")
    statements_df_path = analysis_module.join(name="indranet.tsv")
    processed_path = analysis_module.join(name="data.tsv")
    processed_filtered_path = analysis_module.join(name="data_filtered.tsv")

    target_hgnc_ids = set(target_hgnc_ids)
    df = _read_df(data_path)
    measured = set(df["hgnc"])

    if not statements_pkl_path.is_file():
        pairs = [("hgnc", gene_id) for gene_id in target_hgnc_ids]
        stmts = subnetwork.get_neighbor_network_statements(
            pairs, client=client, node_prefix="hgnc",
            minimum_evidence_count=8,
        )
        statements_pkl_path.write_bytes(pickle.dumps(stmts))
    else:
        stmts = pickle.loads(statements_pkl_path.read_bytes())

    assembler = IndraNetAssembler(stmts)
    stmts_df = assembler.make_df(keep_self_loops=False).sort_values(["agA_name", "agB_name"])
    stmts_df = stmts_df[stmts_df["stmt_type"] != "Complex"]
    stmts_df.drop_duplicates(subset=["stmt_hash"], inplace=True)
    stmts_df.to_csv(statements_df_path, sep='\t', index=False)

    neighbor_hgnc_ids = set(stmts_df["agA_id"]).union(stmts_df["agB_id"])
    df["in_neighbors"] = df["hgnc"].map(neighbor_hgnc_ids.__contains__)
    df.to_csv(processed_path, sep="\t", index=False)

    df[df["in_neighbors"]].to_csv(processed_filtered_path, sep="\t", index=False)


def _read_df(path):
    df = pd.read_csv(path, sep=",")
    columns = list(df.columns)
    columns[0] = "uniprot"
    df.columns = columns
    df["hgnc"] = df["uniprot"].map(uniprot_client.get_hgnc_id)
    df = df[df["hgnc"].notna()]
    df = df[["hgnc", *columns]]
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
    client = Neo4jClient()
    analysis_hgnc(data_path=PATH, target_hgnc_ids=query, analysis_id="simple", client=client)
    analysis_hgnc(data_path=PATH, target_hgnc_ids=get_query("Exploratory_query.csv"), analysis_id="exploratory", client=client)
    analysis_hgnc(data_path=PATH, target_hgnc_ids=get_query("MAPK_downstream.csv"), analysis_id="mapk_downstream", client=client)


if __name__ == '__main__':
    main()
