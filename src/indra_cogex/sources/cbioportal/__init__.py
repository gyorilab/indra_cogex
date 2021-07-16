import pandas as pd

from pathlib import Path
from typing import Union

import pystow
from indra.databases import hgnc_client

from indra_cogex.representation import Node, Relation
from indra_cogex.sources.processor import Processor


class CcleMutationsProcessor(Processor):
    name = "ccle_mutations"

    def __init__(
        self,
        mutations_path: Union[str, Path, None] = None,
    ):
        default_mut_path = pystow.join(
            "indra",
            "cogex",
            "cbioportal",
            "ccle_broad_2019",
            name="data_mutations_extended.txt",
        )
        if not mutations_path:
            mutations_path = default_mut_path
        elif isinstance(mutations_path, str):
            mutations_path = Path(mutations_path)

        self.mutations_df = pd.read_csv(mutations_path, sep="\t", comment="#")

    def get_nodes(self):
        for hgnc_symbol in sorted(set(self.mutations_df["Hugo_Symbol"])):
            hgnc_id = hgnc_client.get_hgnc_id(hgnc_symbol)
            if not hgnc_id:
                continue
            yield Node(db_ns="hgnc", db_id=hgnc_id, labels=["BioEntity"])

        for cell_line in sorted(set(self.mutations_df["Tumor_Sample_Barcode"])):
            yield Node(db_ns="ccle", db_id=cell_line, labels=["BioEntity"])

    def get_relations(self):
        for index, row in self.mutations_df.iterrows():
            if not pd.isna(row["HGVSp_Short"]):
                hgnc_id = hgnc_client.get_hgnc_id(row["Hugo_Symbol"])
                cell_line_id = row["Tumor_Sample_Barcode"]
                if not hgnc_id:
                    continue
                yield Relation(
                    source_ns="hgnc",
                    source_id=hgnc_id,
                    target_ns="ccle",
                    target_id=cell_line_id,
                    rel_type="mutated_in",
                    data={"HGVSp_Short": row["HGVSp_Short"], "source": "ccle"},
                )


class CcleCnaProcessor(Processor):
    name = "ccle_cna"

    def __init__(
        self,
        cna_path: Union[str, Path, None] = None,
    ):
        default_cna_path = pystow.join(
            "indra", "cogex", "cbioportal", "ccle_broad_2019", name="data_CNA.txt"
        )

        if not cna_path:
            cna_path = default_cna_path
        elif isinstance(cna_path, str):
            cna_path = Path(cna_path)

        self.cna_df = pd.read_csv(cna_path, sep="\t")

    def get_nodes(self):
        # Collect all gene symbols from both tables
        for hgnc_symbol in sorted(set(self.cna_df["Hugo_Symbol"])):
            hgnc_id = hgnc_client.get_hgnc_id(hgnc_symbol)
            if not hgnc_id:
                continue
            yield Node(db_ns="hgnc", db_id=hgnc_id, labels=["BioEntity"])

        for cell_line in sorted(set(self.cna_df.columns.values[1:])):
            yield Node(db_ns="ccle", db_id=cell_line, labels=["BioEntity"])

    def get_relations(self):
        for index, row in self.cna_df.iterrows():
            hgnc_id = hgnc_client.get_hgnc_id(row["Hugo_Symbol"])
            if not hgnc_id:
                continue
            for cell_line in self.cna_df.columns.values[1:]:
                if row[cell_line] != 0:
                    yield Relation(
                        source_ns="hgnc",
                        source_id=hgnc_id,
                        target_ns="ccle",
                        target_id=cell_line,
                        rel_type="copy_number_altered_in",
                        data={"CNA": row[cell_line], "source": "ccle"},
                    )
