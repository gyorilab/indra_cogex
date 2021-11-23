import csv
import json
import logging
import pystow
from pathlib import Path
from tqdm.std import tqdm
from indra.util import batch_iter
from indra_cogex.representation import Node, Relation
from indra_cogex.sources.processor import Processor


logger = logging.getLogger(__name__)

resources = pystow.module("indra", "cogex", "pubmed")

MESH_PMID = resources.join(name="mesh_pmids.csv")
PMID_YEAR = resources.join(name="pmid_years_07-2021.json")
TEXT_REFS = resources.join(name="text_refs.tsv")


class PubmedProcessor(Processor):
    name = "pubmed"
    node_types = ["Publication"]

    def __init__(
        self,
        mesh_pmid_path=MESH_PMID,
        pmid_year_path=PMID_YEAR,
        text_refs_path=TEXT_REFS,
    ):
        self.mesh_pmid_path = mesh_pmid_path
        self.pmid_year_path = pmid_year_path
        self.text_refs_path = text_refs_path

    def get_nodes(self, **kwargs):
        pmid_node_type = "Publication"
        logger.info("Loading PMID year info from %s" % self.pmid_year_path)
        with open(self.pmid_year_path, "r") as fh:
            pmid_years = json.load(fh)
        logger.info("Loaded PMID year info from %s" % self.pmid_year_path)

        def get_val(val):
            # postgres exports \N for missing values
            if val == "\\N":
                return None
            else:
                return val

        # We iterate over text refs to get the nodes and
        # then look up the year to add as a property
        with open(self.text_refs_path, "r") as fh:
            reader = csv.reader(fh, delimiter="\t")
            for trid, pmid, pmcid, doi, pii, url, manuscript_id in reader:
                if not get_val(pmid):
                    continue
                year = pmid_years.get(pmid, None)
                data = {
                    "trid": get_val(trid),
                    "pmcid": get_val(pmcid),
                    "doi": get_val(doi),
                    "pii": get_val(pii),
                    "url": get_val(url),
                    "manuscript_id": get_val(manuscript_id),
                    "year": year,
                }
                yield Node("PUBMED", pmid, labels=[pmid_node_type], data=data)

    def get_relations(self):
        with open(self.mesh_pmid_path, "r") as fh:
            reader = csv.reader(fh)
            next(reader)  # skip header
            # NOTE tested with 100000 batch size but given that total is ~290M
            # and each line is lightweight, we could probably try larger batch
            batch_size = 100000
            for batch in tqdm(
                batch_iter(reader, batch_size=batch_size, return_func=list)
            ):
                relations_batch = []
                for mesh_num, is_concept, major_topic, pmid in batch:
                    mesh_id = mesh_num_to_id(mesh_num, int(is_concept))
                    relations_batch.append(
                        Relation(
                            "PUBMED",
                            pmid,
                            "MESH",
                            mesh_id,
                            "annotated_with",
                            {"is_major_topic": True if major_topic == "1" else False},
                        )
                    )
                yield relations_batch

    def _dump_edges(self) -> Path:
        # This overrides the default implementation in Processor because
        # we want to process relations in batches
        for bidx, batch in enumerate(self.get_relations()):
            logger.info(f"Dumping relations batch {bidx}")
            sample_path = None
            write_mode = "at"
            if bidx == 0:
                sample_path = self.module.join(name="edges_sample.tsv")
                write_mode = "wt"
            edges_path = self._dump_edges_to_path(
                batch, self.edges_path, sample_path, write_mode
            )
        return edges_path


def mesh_num_to_id(mesh_num, is_concept):
    prefix = "C" if is_concept else "D"
    if prefix == "D":
        if int(mesh_num) < 66332:
            mesh_num = str(mesh_num).zfill(6)
        else:
            mesh_num = str(mesh_num).zfill(9)
    elif prefix == "C":
        if int(mesh_num) < 588418:
            mesh_num = str(mesh_num).zfill(6)
        else:
            mesh_num = str(mesh_num).zfill(9)
    return prefix + mesh_num
