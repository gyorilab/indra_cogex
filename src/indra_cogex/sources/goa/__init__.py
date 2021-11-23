# -*- coding: utf-8 -*-

"""Processor for the Gene Ontology Associations (GOA) database."""

import logging

import pandas as pd

from indra.databases import uniprot_client
from ..processor import Processor
from ...representation import Node, Relation

logger = logging.getLogger(__name__)

GOA_URL = "http://geneontology.org/gene-associations/goa_human.gaf.gz"
EVIDENCE_CODES = {
    "EXP",
    "IDA",
    "IPI",
    "IMP",
    "IGI",
    "IEP",
    "HTP",
    "HDA",
    "HMP",
    "HGI",
    "HEP",
    "IBA",
    "IBD",
}


class GoaProcessor(Processor):
    """Processor for the Gene Ontology Associations (GOA) database."""

    name = "goa"
    df: pd.DataFrame
    node_types = ["BioEntity"]

    def __init__(self):
        """Initialize the GOA processor."""
        self.df = load_goa(GOA_URL)

    def get_nodes(self):  # noqa:D102
        for go_node in self.df["GO_ID"].unique():
            yield Node("GO", go_node, ["BioEntity"])
        for hgnc_id in self.df["HGNC_ID"].unique():
            yield Node("HGNC", hgnc_id, ["BioEntity"])

    def get_relations(self):  # noqa:D102
        rel_type = "associated_with"
        for (go_id, hgnc_id), ecs in self.df.groupby(["GO_ID", "HGNC_ID"])["EC"]:
            all_ecs = ",".join(sorted(set(ecs)))
            # Possible properties could be e.g., evidence codes
            data = {"evidence_codes:string": all_ecs, "source": self.name}
            yield Relation("HGNC", hgnc_id, "GO", go_id, rel_type, data)


def load_goa(url: str) -> pd.DataFrame:
    """Get the Gene Ontology Annotations database as a dataframe.

    :param url: The URL to the GOA database file.
    :return: The GOA database as a dataframe
    """
    logger.info("Loading GO annotations from %s", url)
    df = pd.read_csv(
        url,
        sep="\t",
        comment="!",
        dtype=str,
        header=None,
        usecols=[1, 3, 4, 6],
        names=[
            "UP_ID",
            "Qualifier",
            "GO_ID",
            "EC",
        ],
    )
    df["HGNC_ID"] = df.apply(
        lambda row: uniprot_client.get_hgnc_id(row["UP_ID"]),
        axis=1,
    )
    df = df[~df["HGNC_ID"].isna()]
    df["Qualifier"].fillna("", inplace=True)
    df = df[~df["Qualifier"].str.startswith("NOT")]
    df = df[df["EC"].isin(EVIDENCE_CODES)]
    logger.info("Loaded %s rows", len(df))
    return df
