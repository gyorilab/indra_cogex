"""Run Vitek lab analysis - get PKN based on mass spec results."""

import json
import logging
from operator import itemgetter
from pathlib import Path
from textwrap import dedent
from typing import Iterable, Optional, Set, Union

import bioregistry
import click
import pandas as pd
import pystow
from indra.databases.hgnc_client import get_hgnc_name, get_uniprot_id
from protmapper import uniprot_client

from indra_cogex.client import Neo4jClient, autoclient
from indra_cogex.client.utils import minimum_evidence_helper

logger = logging.getLogger(__name__)
HERE = Path(__file__).parent.resolve()
PATH = HERE.joinpath("devon/example_data.csv")
OUTPUT_MODULE = pystow.module("indra", "cogex", "analysis", "devon")


def get_query_from_file(fname: str) -> Set[str]:
    """Get the query UniProt set from one of the files in this directory by name."""
    prefix, *lines = HERE.joinpath(fname).read_text().splitlines()
    lines = {line.strip() for line in lines if line.strip()}
    norm_prefix = bioregistry.normalize_prefix(prefix)
    if norm_prefix is None:
        raise ValueError(f"invalid prefix: {prefix}")
    if norm_prefix == "hgnc":
        return {
            uniprot_id.strip()
            for hgnc_id in lines
            for uniprot_id in get_uniprot_id(hgnc_id).split(",")
        }
    elif norm_prefix == "uniprot":
        return lines
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
        return {
            uniprot_id.strip()
            for hgnc_id in lines
            for uniprot_id in get_uniprot_id(hgnc_id).split(",")
        }
    elif column.lower().replace("_", "").removesuffix("id") in {"uniprot", "uniprotkb"}:
        return lines
    else:
        raise ValueError(f"unhandled column: {column}")


@autoclient()
def analysis_uniprot(
    uniprot_ids: Iterable[str],
    *,
    data_path: Optional[Path] = None,
    analysis_id: str,
    client: Optional[Neo4jClient] = None,
    minimum_evidence_count: Optional[int] = None,
):
    """
    This analysis takes a list of target proteins and gets a subnetwork
    of INDRA increase amount and decrease amount statements where
    both the source and target are in the list. It uses a default
    minimum evidence count of 8, which filters out the most obscure
    and likely incorrect statements (e.g., due to technical reader errors).

    .. todo::

        - consider direct versus indirect interactions (i.e., is physical contact
          involved). Should we remove these, or try and keep these "bypass" edges?
        - consider cycles. Look at canonical-ness, e.g., use curated pathway databases
          as a guide to figure out what the "forward" pathways are. This can be
          thought of like an optimization problem - what is the smallest set of least
          important edges to remove.
        - small cycles where A->B and B->A.
    """
    logger.info("running analysis: %s", analysis_id)
    analysis_module = OUTPUT_MODULE.module(analysis_id)

    if minimum_evidence_count is None:
        minimum_evidence_count = 8
        edges_path = analysis_module.join(name="edges.tsv")
        data_annotated_path = analysis_module.join(name="data.tsv")
        data_annotated_filtered_path = analysis_module.join(name="data_filtered.tsv")
        density_path = analysis_module.join(name="density.tsv")
    else:
        edges_path = analysis_module.join(name=f"{minimum_evidence_count:03}_edges.tsv")
        data_annotated_path = analysis_module.join(name=f"{minimum_evidence_count:03}_data.tsv")
        data_annotated_filtered_path = analysis_module.join(
            name=f"{minimum_evidence_count:03}_data_filtered.tsv"
        )
        density_path = analysis_module.join(name=f"{minimum_evidence_count:03}_density.tsv")

    uniprot_ids = set(uniprot_ids)
    click.echo(f"Querying {len(uniprot_ids):,} UniProt identifiers")

    def _get_uniprot_from_hgnc(hgnc_id: str) -> Optional[str]:
        uniprot_id = get_uniprot_id(hgnc_id)
        if "," not in uniprot_id:
            return uniprot_id
        for uu in uniprot_id.split(","):
            if uu in uniprot_ids:
                return uu
        return None

    hgnc_ids = set()
    failed = set()
    for uniprot_id in uniprot_ids:
        hgnc_id = uniprot_client.get_hgnc_id(uniprot_id)
        if hgnc_id:
            hgnc_ids.add(hgnc_id)
        else:
            failed.add(uniprot_id)
    if failed:
        click.echo(f"Failed to get HGNC ID for UniProts: {sorted(failed)}")

    hgnc_curies = [f"hgnc:{gene_id}" for gene_id in hgnc_ids]

    density_query = dedent(
        f"""\
            MATCH p=(n1:BioEntity)-[r:indra_rel]->(n2:BioEntity)
            WHERE 
                n1.id IN {hgnc_curies!r}
                AND n2.id STARTS WITH 'hgnc'
                AND n1.id <> n2.id
                AND r.stmt_type IN ['IncreaseAmount', 'DecreaseAmount']
                {minimum_evidence_helper(minimum_evidence_count)}
            RETURN n1.id, count(distinct n2.id)
        """
    )
    density = []
    for curie, count in sorted(client.query_tx(density_query), key=itemgetter(1), reverse=True):
        hgnc_id = curie.removeprefix("hgnc:")

        density.append(
            (
                hgnc_id,
                get_hgnc_name(hgnc_id),
                _get_uniprot_from_hgnc(hgnc_id),
                count,
            )
        )
    columns = ["hgnc_id", "hgnc_symbol", "uniprot_id", "unique_hgnc_neighbors"]
    pd.DataFrame(density, columns=columns).to_csv(density_path, sep="\t", index=False)

    query = dedent(
        f"""\
            MATCH p=(n1:BioEntity)-[r:indra_rel]->(n2:BioEntity)
            WHERE 
                n1.id IN {hgnc_curies!r}
                AND n2.id IN {hgnc_curies!r}
                AND n1.id <> n2.id
                AND r.stmt_type IN ['IncreaseAmount', 'DecreaseAmount']
                {minimum_evidence_helper(minimum_evidence_count)}
            RETURN p
        """
    )

    columns = [
        "source_hgnc_id",
        "source_hgnc_symbol",
        "source_uniprot_id",
        "relation",
        "target_hgnc_id",
        "target_hgnc_symbol",
        "target_uniprot_id",
        "stmt_hash",
        "evidence_count",
        "source_counts",
    ]

    rows = []
    skipped = 0
    for relation in client.query_relations(query):
        source_uniprot = _get_uniprot_from_hgnc(relation.source_id)
        target_uniprot = _get_uniprot_from_hgnc(relation.target_id)
        if not source_uniprot or not target_uniprot:
            skipped += 1
            continue
        rows.append(
            (
                relation.source_id,
                get_hgnc_name(relation.source_id),
                source_uniprot,
                relation.data["stmt_type"],
                relation.target_id,
                get_hgnc_name(relation.target_id),
                target_uniprot,
                relation.data["stmt_hash"],
                sum(json.loads(relation.data["source_counts"]).values()),
                relation.data["source_counts"],
            )
        )

    click.echo(f"Skipped {skipped:,} rows due to problematic UniProt->HGNC->UniProt round trip")

    df = pd.DataFrame(rows, columns=columns)
    df.drop_duplicates(subset=["stmt_hash"], inplace=True)
    click.echo(f"writing edges to {edges_path}")
    df.to_csv(edges_path, sep="\t", index=False)

    if data_path is not None:
        data_df = _read_df(data_path)
        # measured = set(df["hgnc"])
        neighbor_hgnc_ids = set(df["source_hgnc_id"]).union(df["target_hgnc_id"])
        data_df["in_neighbors"] = data_df["hgnc"].map(neighbor_hgnc_ids.__contains__)
        data_df.to_csv(data_annotated_path, sep="\t", index=False)
        logger.info(f"Writing results to {data_annotated_filtered_path}")
        data_df[data_df["in_neighbors"]].to_csv(data_annotated_filtered_path, sep="\t", index=False)


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
    query = {get_uniprot_id(hgnc_id) for hgnc_id in query}
    client = Neo4jClient()
    analysis_uniprot(data_path=PATH, uniprot_ids=query, analysis_id="simple", client=client)
    analysis_uniprot(
        uniprot_ids=get_query_from_tsv("vartika/protids.csv", column="UniProtID"),
        analysis_id="bertis",
        client=client,
    )
    analysis_uniprot(
        uniprot_ids=get_query_from_tsv("vartika/OV_ruth_1.csv", column="UniprotID"),
        analysis_id="huettenhain",
        client=client,
    )
    analysis_uniprot(
        uniprot_ids=get_query_from_xlsx("vartika/CRC_silvia_1.xlsx", column="UniprotID"),
        analysis_id="surinova",
        client=client,
    )
    for minimum_evidence_count in [1, 8, 50, 100, 150]:
        analysis_uniprot(
            data_path=PATH,
            uniprot_ids=get_query_from_tsv("devon/slavov.csv", column="uniprot"),
            analysis_id="slavov",
            client=client,
            minimum_evidence_count=minimum_evidence_count,
        )
    # Devon said the following two are just permutations of the Slavov data
    analysis_uniprot(
        data_path=PATH,
        uniprot_ids=get_query_from_file("devon/Exploratory_query.csv"),
        analysis_id="exploratory",
        client=client,
    )
    analysis_uniprot(
        data_path=PATH,
        uniprot_ids=get_query_from_file("devon/MAPK_downstream.csv"),
        analysis_id="mapk_downstream",
        client=client,
    )


if __name__ == "__main__":
    main()
