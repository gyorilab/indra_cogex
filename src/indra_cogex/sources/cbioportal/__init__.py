import re
import tarfile
import logging

import pandas as pd

from pathlib import Path
from typing import Union

import gilda
import pystow
from indra.databases import hgnc_client

from indra_cogex.representation import Node, Relation
from indra_cogex.sources.processor import Processor

logger = logging.getLogger(__name__)


class CcleMutationsProcessor(Processor):
    name = "ccle_mutations"
    node_types = ["BioEntity"]

    def __init__(
        self,
        path: Union[str, Path, None] = None,
    ):
        if not path:
            tar_path = get_data()
            with tarfile.open(tar_path, "r") as fh:
                with fh.extractfile("ccle_broad_2019/data_mutations.txt") as fhh:
                    self.df = pd.read_csv(fhh, sep="\t", comment="#")
        else:
            if isinstance(path, str):
                path = Path(path)
            self.df = pd.read_csv(path, sep="\t", comment="#")

    def get_nodes(self):
        for hgnc_symbol in sorted(set(self.df["Hugo_Symbol"])):
            hgnc_id = hgnc_client.get_hgnc_id(hgnc_symbol)
            if not hgnc_id:
                continue
            yield Node(db_ns="HGNC", db_id=hgnc_id, labels=["BioEntity"])

        for cell_line in sorted(set(self.df["Tumor_Sample_Barcode"])):
            yield Node(db_ns="CCLE", db_id=cell_line, labels=["BioEntity"])

    def get_relations(self):
        for index, row in self.df.iterrows():
            if not pd.isna(row["HGVSp_Short"]):
                hgnc_id = hgnc_client.get_hgnc_id(row["Hugo_Symbol"])
                cell_line_id = row["Tumor_Sample_Barcode"]
                if not hgnc_id:
                    continue
                yield Relation(
                    source_ns="HGNC",
                    source_id=hgnc_id,
                    target_ns="CCLE",
                    target_id=cell_line_id,
                    rel_type="mutated_in",
                    data={"HGVSp_Short": row["HGVSp_Short"], "source": "ccle"},
                )


class CcleCnaProcessor(Processor):
    name = "ccle_cna"
    node_types = ["BioEntity"]

    def __init__(
        self,
        path: Union[str, Path, None] = None,
    ):
        if not path:
            tar_path = get_data()
            with tarfile.open(tar_path, "r") as fh:
                with fh.extractfile("ccle_broad_2019/data_cna.txt") as fhh:
                    self.df = pd.read_csv(fhh, sep="\t")
        else:
            if isinstance(path, str):
                path = Path(path)
            self.df = pd.read_csv(path, sep="\t")

    def get_nodes(self):
        # Collect all gene symbols from both tables
        for hgnc_symbol in sorted(set(self.df["Hugo_Symbol"])):
            hgnc_id = hgnc_client.get_hgnc_id(hgnc_symbol)
            if not hgnc_id:
                continue
            yield Node(db_ns="HGNC", db_id=hgnc_id, labels=["BioEntity"])

        for cell_line in sorted(set(self.df.columns.values[1:])):
            yield Node(db_ns="CCLE", db_id=cell_line, labels=["BioEntity"])

    def get_relations(self):
        for index, row in self.df.iterrows():
            hgnc_id = hgnc_client.get_hgnc_id(row["Hugo_Symbol"])
            if not hgnc_id:
                continue
            for cell_line in self.df.columns.values[1:]:
                if row[cell_line] != 0:
                    yield Relation(
                        source_ns="HGNC",
                        source_id=hgnc_id,
                        target_ns="CCLE",
                        target_id=cell_line,
                        rel_type="copy_number_altered_in",
                        data={"CNA:int": row[cell_line], "source": "ccle"},
                    )


class CcleDrugResponseProcessor(Processor):
    name = "ccle_drug"
    node_types = ["BioEntity"]

    def __init__(self, path: Union[str, Path, None] = None):
        if not path:
            tar_path = get_data()
            with tarfile.open(tar_path, "r") as fh:
                with fh.extractfile(
                    "ccle_broad_2019/data_drug_treatment_ic50.txt"
                ) as fhh:
                    self.df = pd.read_csv(fhh, sep="\t")
        else:
            if isinstance(path, str):
                path = Path(path)
            self.df = pd.read_csv(path, sep="\t")
        self.drug_mappings = {}

    def get_nodes(self):
        drugs = self.get_drug_mappings()
        for db_ns, db_id, name in drugs.values():
            if db_ns and db_id:
                yield Node(
                    db_ns=db_ns,
                    db_id=db_id,
                    labels=["BioEntity"],
                    data={"name": name}
                )

        for cell_line in list(self.df.columns[5:]):
            yield Node("CCLE", cell_line, labels=["BioEntity"])

    def get_relations(self):
        cell_lines = self.df.columns[5:]
        for _, row in self.df.iterrows():
            drug = row["ENTITY_STABLE_ID"]
            drug_ns, drug_id, _ = self.drug_mappings.get(drug, (None, None, None))
            if drug_ns and drug_id:
                for cell_line in cell_lines:
                    if not pd.isna(row[cell_line]) and row[cell_line] < 10:
                        yield Relation(
                            "CCLE",
                            cell_line,
                            drug_ns,
                            drug_id,
                            rel_type="sensitive_to",
                            data={
                                "IC50:float": row[cell_line],
                                "source": "ccle"
                            },
                        )

    def get_drug_mappings(self):
        self.drug_mappings = {}
        for _, row in self.df.iterrows():
            # We skip ones of the form "Afatinib 1/2" because we use the
            # corresponding "Afatinib 2/2" entries instead.
            if re.match(r"^(.+) 1/2$", row["NAME"]):
                continue
            elif re.match(r"^(.+) 2/2$", row["NAME"]):
                to_ground = [row["ENTITY_STABLE_ID"].rsplit("-", 1)[0]]
            else:
                to_ground = [row["ENTITY_STABLE_ID"]]

            match = re.search(r"Synonyms:(.+)", row["DESCRIPTION"])
            if match:
                syns = match.groups()[0]
                if syns != "None":
                    to_ground += [syn.strip() for syn in syns.split(",")]

            db_ns, db_id, name = self.ground_drug(to_ground)
            self.drug_mappings[row["ENTITY_STABLE_ID"]] = (db_ns, db_id, name)
        return self.drug_mappings

    def ground_drug(self, names):
        for name in names:
            matches = gilda.ground(name)
            if matches:
                best_term = matches[0].term
                return best_term.db, best_term.id, best_term.entry_name
        logger.info("Could not match %s" % str(names))
        return None, None, None


def get_data():
    url = "https://cbioportal-datahub.s3.amazonaws.com/ccle_broad_2019.tar.gz"
    return pystow.ensure(
        "indra", "cogex", "cbioportal", name="ccle_broad_2019.tar.gz", url=url
    )
