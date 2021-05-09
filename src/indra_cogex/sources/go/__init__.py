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
        logger.info("Loaded %s rows from %s", len(self.df), GOA_URL)

    def get_nodes(self):
        return
        yield

    def get_relations(self):
        rel_type = "associated_with"
        for _, row in self.df.iterrows():
            up_id = row[1]
            go_id = row[4]
            hgnc_id = uniprot_client.get_hgnc_id(up_id)
            if not hgnc_id:
                continue
            source = f"HGNC:{hgnc_id}"
            # Note that we don't add the extra GO: by current convention
            target = go_id
            # Possible properties could be e.g., evidence codes
            data = {}
            yield Relation(source, target, [rel_type], data)


def load_goa(url):
    df = pd.read_csv(url, sep="\t", skiprows=41, dtype=str, header=None)
    df[3].fillna("", inplace=True)
    df = df[~df[3].str.startswith("NOT")]
    df = df[df[6].isin(EVIDENCE_CODES)]
    return df
