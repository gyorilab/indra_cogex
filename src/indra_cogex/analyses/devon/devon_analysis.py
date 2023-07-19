"""Run Vitek lab analysis - get PKN based on mass spec results."""

import logging
import pickle
from pathlib import Path
from typing import Iterable, Optional, Set, Union

import bioregistry
import click
import pandas as pd
import pystow
from indra.assemblers.indranet import IndraNetAssembler
from protmapper import uniprot_client
from protmapper.api import hgnc_id_to_up

from indra_cogex.client import Neo4jClient, autoclient, subnetwork

logger = logging.getLogger(__name__)
HERE = Path(__file__).parent.resolve()
PATH = HERE.joinpath("devon/example_data.csv")
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
        return {uniprot_client.get_hgnc_id(line) for line in lines}
    else:
        raise ValueError(f"unhandled prefix: {norm_prefix}")


def get_query_from_tsv(fname, *, column, sep=",") -> Set[str]:
    """Get a gene list query from a TSV file"""
    df = pd.read_csv(fname, sep=sep)
    return _get_query_from_df(df, column=column)


def get_query_from_xlsx(fname, *, column: str) -> Set[str]:
    df = pd.read_excel(fname)
    return _get_query_from_df(df, column=column)


def _get_query_from_df(df: pd.DataFrame, column) -> Set[str]:
    df = df[df[column].notna()]
    lines = set(df[column])
    if column.lower().replace("_", "").removesuffix("id") == "hgnc":
        return lines
    elif column.lower().replace("_", "").removesuffix("id") in {"uniprot", "uniprotkb"}:
        rv = {uniprot_client.get_hgnc_id(line) for line in lines}
        return {r for r in rv if r}
    else:
        raise ValueError(f"unhandled column: {column}")


@autoclient()
def analysis_hgnc(
    target_hgnc_ids: Iterable[str],
    *,
    data_path: Optional[Path] = None,
    analysis_id: str,
    client: Optional[Neo4jClient] = None,
    minimum_evidence_count: int = 8,
):
    logger.info("running analysis: %s", analysis_id)
    analysis_module = OUTPUT_MODULE.module(analysis_id)
    statements_pkl_path = analysis_module.join(name="statements.pkl")
    statements_df_path = analysis_module.join(name="indranet.tsv")
    processed_path = analysis_module.join(name="data.tsv")

    target_hgnc_ids = set(target_hgnc_ids)

    if not statements_pkl_path.is_file():
        pairs = [("hgnc", gene_id) for gene_id in target_hgnc_ids]
        stmts = subnetwork.get_neighbor_network_statements(
            pairs,
            client=client,
            node_prefix="hgnc",
            minimum_evidence_count=minimum_evidence_count,
        )
        statements_pkl_path.write_bytes(pickle.dumps(stmts))
    else:
        stmts = pickle.loads(statements_pkl_path.read_bytes())

    assembler = IndraNetAssembler(stmts)
    exc = []
    stmts_df = assembler.make_df(keep_self_loops=False).sort_values(["agA_name", "agB_name"])
    for side in "AB":
        side_uniprot_ids = []
        for side_ns, side_id in stmts_df[[f"ag{side}_ns", f"ag{side}_id"]].values:
            if side_ns == "HGNC":
                uniprot_id = hgnc_id_to_up.get(str(side_id))
            else:
                uniprot_id = None
            side_uniprot_ids.append(uniprot_id)
        stmts_df[f"ag{side}_uniprot"] = side_uniprot_ids

    stmts_df = stmts_df[stmts_df["stmt_type"] != "Complex"]
    stmts_df.drop_duplicates(subset=["stmt_hash"], inplace=True)
    logger.info(f"writing INDRANet to {statements_df_path}")
    stmts_df.to_csv(statements_df_path, sep="\t", index=False)

    if data_path is not None:
        data_df = _read_df(data_path)
        # measured = set(df["hgnc"])
        neighbor_hgnc_ids = set(stmts_df["agA_id"]).union(stmts_df["agB_id"])
        data_df["in_neighbors"] = data_df["hgnc"].map(neighbor_hgnc_ids.__contains__)
        data_df.to_csv(processed_path, sep="\t", index=False)
        processed_filtered_path = analysis_module.join(name="data_filtered.tsv")
        logger.info(f"Writing results to {processed_filtered_path}")
        data_df[data_df["in_neighbors"]].to_csv(processed_filtered_path, sep="\t", index=False)


def _read_df(path: Union[str, Path]) -> pd.DataFrame:
    path = Path(path).resolve()
    df = pd.read_csv(path, sep=",")
    columns = list(df.columns)
    columns[0] = "uniprot"
    df.columns = columns
    df["hgnc"] = df["uniprot"].map(uniprot_client.get_hgnc_id)
    df = df[df["hgnc"].notna()]
    df = df[["hgnc", *columns]]
    return df


@click.command()
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
    analysis_hgnc(
        target_hgnc_ids=get_query_from_tsv("vartika/protids.csv", column="UniProtID"),
        analysis_id="bertis",
        client=client,
    )
    return
    analysis_hgnc(
        target_hgnc_ids=get_query_from_tsv("vartika/OV_ruth_1.csv", column="UniprotID"),
        analysis_id="huettenhain",
        client=client,
    )
    analysis_hgnc(
        target_hgnc_ids=get_query_from_xlsx("vartika/CRC_silvia_1.xlsx", column="UniprotID"),
        analysis_id="surinova",
        client=client,
    )
    analysis_hgnc(
        data_path=PATH,
        target_hgnc_ids=get_query("devon/Exploratory_query.csv"),
        analysis_id="exploratory",
        client=client,
    )
    analysis_hgnc(
        data_path=PATH,
        target_hgnc_ids=get_query_from_tsv("devon/gene_list.csv", column="uniprot"),
        analysis_id="slavov",
        client=client,
    )
    analysis_hgnc(
        data_path=PATH,
        target_hgnc_ids=get_query("devon/MAPK_downstream.csv"),
        analysis_id="mapk_downstream",
        client=client,
    )
    analysis_hgnc(
        data_path=PATH,
        target_hgnc_ids=get_query("devon/MAPK_downstream.csv"),
        analysis_id="mapk_downstream",
        client=client,
    )


if __name__ == "__main__":
    main()
