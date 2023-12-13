# -*- coding: utf-8 -*-

"""Process DisGeNet, a resource for gene-disease and variant-disease associations."""

import logging
import pickle

import click
import pandas as pd
import pystow
from indra.databases import hgnc_client

from indra_cogex.representation import Node, Relation
from indra_cogex.sources import Processor
from indra_cogex.sources.utils import UmlsMapper

__all__ = [
    "DisgenetProcessor",
]

logger = logging.getLogger(__name__)

SUBMODULE = pystow.module("indra", "cogex", "disgenet")

DOWNLOAD_BASE = "https://www.disgenet.org/static/disgenet_ap1/files/downloads"
CURATED_DISEASE_GENES_ASSOCIATIONS_URL = (
    f"{DOWNLOAD_BASE}/curated_gene_disease_associations.tsv.gz"
)
CURATED_DISEASE_VARIANT_ASSOCIATIONS_URL = (
    f"{DOWNLOAD_BASE}/curated_variant_disease_associations.tsv.gz"
)
CURATED_VARIANT_GENE_ASSOCIATIONS_URL = (
    f"{DOWNLOAD_BASE}/variant_to_gene_mappings.tsv.gz"
)


class DisgenetProcessor(Processor):
    """Processor for the DisGeNet database."""

    name = "disgenet"
    node_types = ["BioEntity"]
    gene_relation = "gene_disease_association"
    variant_relation = "variant_disease_association"
    variant_gene_relation = "variant_gene_association"

    def __init__(self):
        """Initialize the DisGeNet processor."""
        self.gene_df = load_disgenet_disease_gene(
            CURATED_DISEASE_GENES_ASSOCIATIONS_URL
        )
        self.variant_df = load_disgenet_disease_variant(
            CURATED_DISEASE_VARIANT_ASSOCIATIONS_URL
        )

        self.variant_gene_df = load_disgenet_variant_gene(
            CURATED_VARIANT_GENE_ASSOCIATIONS_URL
        )

    def get_nodes(self):  # noqa:D102
        diseases = {
            tuple(row)
            for df in [self.gene_df, self.variant_df]
            for row in df[["disease_prefix", "disease_id", "disease_name"]].values
        }
        for prefix, identifier, name in diseases:
            yield Node.standardized(
                db_ns=prefix, db_id=identifier, name=name, labels=["BioEntity"]
            )
        for hgnc_id in self.gene_df["hgnc_id"].unique():
            yield Node.standardized(db_ns="HGNC", db_id=hgnc_id, labels=["BioEntity"])

        for dbsnp_id in self.variant_df["snpId"].unique():
            yield Node.standardized(
                db_ns="DBSNP", db_id=dbsnp_id, name=dbsnp_id, labels=["BioEntity"]
            )

    def get_relations(self):  # noqa:D102
        yield from _yield_gene_relations(self.gene_df, self.name, self.gene_relation)
        yield from _yield_variant_relations(
            self.variant_df, self.name, self.variant_relation
        )
        yield from _yield_variant_gene_relations(
            self.variant_gene_df, self.name, self.variant_gene_relation
        )


def _yield_gene_relations(df, name, relation):
    columns = [
        "hgnc_id",
        "disease_prefix",
        "disease_id",
        "DSI",
        "DPI",
        "score",
        "NofSnps",
        "NofPmids",
    ]

    for hgnc_id, disease_prefix, disease_id, dsi, dpi, snps, score, papers in (
        df[columns].drop_duplicates().values
    ):
        data = {"snps:int": snps, "source": name, "papers:int": papers}
        if pd.notna(dsi):
            data["disgenet_dsi:float"] = dsi
        if pd.notna(dpi):
            data["disgenet_dpi:float"] = dpi
        if pd.notna(score):
            data["disgenet_score:float"] = score
        yield Relation("HGNC", hgnc_id, disease_prefix, disease_id, relation, data)


def _yield_variant_relations(df, name, relation):
    columns = [
        "snpId",
        "DSI",
        "DPI",
        "score",
        "disease_prefix",
        "disease_id",
        "NofPmids",
    ]
    for snp_id, dsi, dpi, score, disease_prefix, disease_id, papers in (
        df[columns].drop_duplicates().values
    ):
        data = {
            "source": name,
            "papers:int": papers,
        }
        if pd.notna(dsi):
            data["disgenet_dsi:float"] = dsi
        if pd.notna(dpi):
            data["disgenet_dpi:float"] = dpi
        if pd.notna(score):
            data["disgenet_score:float"] = score
        yield Relation("DBSNP", snp_id, disease_prefix, disease_id, relation, data)


def _yield_variant_gene_relations(df, name, relation):
    columns = ["snpId", "hgnc_id"]
    for snp_id, hgnc_id in df[columns].drop_duplicates().values:
        data = {"source": name}
        yield Relation("DBSNP", snp_id, "HGNC", hgnc_id, relation, data)


def load_disgenet_disease_gene(
    url,
    force: bool = False,
) -> pd.DataFrame:
    """Export disease-gene association file."""
    df = SUBMODULE.ensure_csv(
        url=url,
        read_csv_kwargs=dict(dtype={"geneId": str}),
        force=force,
    )
    _map_disease(df)
    # Filter out ungroundable
    df = df[df["disease_prefix"].notna()]
    _map_entrez(df)
    df = df[df["hgnc_id"].notna()]
    return df


def load_disgenet_disease_variant(
    url,
    force: bool = False,
) -> pd.DataFrame:
    df = SUBMODULE.ensure_csv(
        url=url,
        read_csv_kwargs=dict(dtype={"snpId": str}),
        force=force,
    )
    _map_disease(df)
    # Filter out ungroundable
    df = df[df["disease_prefix"].notna()]
    return df


def _map_disease(df):
    mapper_path = SUBMODULE.join(name="umls_mapper.pkl")
    if mapper_path.is_file():
        click.echo("loading UMLS mapper")
        umls_mapper = pickle.loads(mapper_path.read_bytes())
        click.echo("done loading UMLS mapper")
    else:
        click.echo("loading UMLS mapper")
        umls_mapper = UmlsMapper()
        click.echo("writing UMLS mapper")
        mapper_path.write_bytes(
            pickle.dumps(umls_mapper, protocol=pickle.HIGHEST_PROTOCOL)
        )
        click.echo("done writing UMLS mapper")

    click.echo("mapping UMLS")

    (
        df["disease_prefix"],
        df["disease_id"],
        df["disease_name"],
    ) = zip(*df["diseaseId"].map(umls_mapper.standardize))
    click.echo("done mapping UMLS")


def load_disgenet_variant_gene(
    url,
    force: bool = False,
) -> pd.DataFrame:
    df = SUBMODULE.ensure_csv(
        url=url,
        read_csv_kwargs=dict(dtype={"geneId": str, "snpId": str}),
        force=force,
    )
    _map_entrez(df)
    df = df[df["hgnc_id"].notna()]
    return df


def _map_entrez(df):
    click.echo("mapping HGNC")
    df["hgnc_id"] = df["geneId"].map(
        lambda s: hgnc_client.get_hgnc_from_entrez(s.strip())
    )
    click.echo("done mapping HGNC")
