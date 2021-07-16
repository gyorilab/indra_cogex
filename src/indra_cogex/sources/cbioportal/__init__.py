import pandas as pd

from pathlib import Path
from typing import Union

import gilda
import pystow
from indra.databases import hgnc_client

from indra_cogex.representation import Node, Relation
from indra_cogex.sources.processor import Processor


class CcleMutationsProcessor(Processor):
    name = "ccle_mutations"

    def __init__(
        self,
        path: Union[str, Path, None] = None,
    ):
        default_path = pystow.join(
            "indra",
            "cogex",
            "cbioportal",
            "ccle_broad_2019",
            name="data_mutations_extended.txt",
        )
        if not path:
            path = default_path
        elif isinstance(path, str):
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

    def __init__(
        self,
        path: Union[str, Path, None] = None,
    ):
        default_path = pystow.join(
            "indra", "cogex", "cbioportal", "ccle_broad_2019", name="data_CNA.txt"
        )

        if not path:
            path = default_path
        elif isinstance(path, str):
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
                        data={"CNA": row[cell_line], "source": "ccle"},
                    )


class CcleDrugResponseProcessor(Processor):
    name = "ccle_drug"

    def __init__(self, path: Union[str, Path, None] = None):

        default_path = pystow.join(
            "indra",
            "cogex",
            "cbioportal",
            "ccle_broad_2019",
            name="data_drug_treatment_IC50",
        )

        if not path:
            path = default_path
        elif isinstance(path, str):
            path = Path(path)

        self.df = pd.read_csv(path, sep="\t")
        self.drug_mappings = {}

    def get_nodes(self):
        for drug in list(self.df["ENTITY_STABLE_ID"]):
            db_ns, db_id = self.ground_drug(drug)
            if db_ns and db_id:
                yield Node(db_ns, db_id, labels=["BioEntity"])

        for cell_line in list(self.df.columns[5:]):
            yield Node("CCLE", cell_line, labels=["BioEntity"])

    def get_relations(self):
        cell_lines = self.df.columns[5:]
        for _, row in self.df.iterrows():
            drug = row["ENTITY_STABLE_ID"]
            drug_ns, drug_id = self.ground_drug(drug)
            if drug_ns and drug_id:
                for cell_line in cell_lines:
                    if not pd.isna(row[cell_line]) and row[cell_line] < 10:
                        yield Relation(
                            "CCLE",
                            cell_line,
                            drug_ns,
                            drug_id,
                            rel_type="sensitive_to",
                            data={"IC50": row[cell_line]},
                        )

    def ground_drug(self, std_id, name=None, synonyms=None):
        cached_grounding = self.drug_mappings.get(std_id)
        if cached_grounding:
            return cached_grounding
        matches = gilda.ground(std_id)
        if matches:
            db_ns, db_id = matches[0].term.db, matches[0].term.id
        else:
            db_ns, db_id = None, None
        self.drug_mappings[std_id] = (db_ns, db_id)
        return db_ns, db_id
