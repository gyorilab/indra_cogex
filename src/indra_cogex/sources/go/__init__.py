import logging
import pandas as pd
from ..processor import Processor
from ...representation import Node, Relation
from indra.databases import uniprot_client

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


class GoProcessor(Processor):
    name = "go"
    df: pd.DataFrame

    def __init__(self):
        self.df = load_goa(GOA_URL)

    def get_nodes(self):
        for go_node in self.df["GO_ID"].unique():
            yield Node(go_node, ["BioEntity"])
        for hgnc_id in self.df["HGNC_ID"].unique():
            if not hgnc_id:
                continue
            yield Node(f"HGNC:{hgnc_id}", ["BioEntity"])

    def get_relations(self):
        rel_type = "associated_with"
        for _, row in self.df.iterrows():
            hgnc_id = row["HGNC_ID"]
            if not hgnc_id:
                continue
            go_id = row["GO_ID"]
            source = f"HGNC:{hgnc_id}"
            # Note that we don't add the extra GO: by current convention
            target = go_id
            # Possible properties could be e.g., evidence codes
            data = {}
            yield Relation(source, target, [rel_type], data)


def load_goa(url):
    logger.info("Loading GO annotations from %s", url)
    df = pd.read_csv(url, sep="\t", skiprows=41, dtype=str, header=None)
    logger.info("Processing GO annotations table")
    df.rename(
        columns={
            1: "UP_ID",
            3: "Qualifier",
            4: "GO_ID",
            6: "EC",
        },
        inplace=True,
    )
    df["HGNC_ID"] = df.apply(
        lambda row: uniprot_client.get_hgnc_id(row["UP_ID"]),
        axis=1,
    )
    df["Qualifier"].fillna("", inplace=True)
    df = df[~df["Qualifier"].str.startswith("NOT")]
    df = df[df["EC"].isin(EVIDENCE_CODES)]
    logger.info("Loaded %s rows", len(df))
    return df
