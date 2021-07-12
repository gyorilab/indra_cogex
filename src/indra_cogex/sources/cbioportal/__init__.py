import pandas as pd
import os

from pathlib import Path
from typing import Union

from indra.databases import hgnc_client

from indra_cogex.representation import Node, Relation
from indra_cogex.sources.processor import Processor


class CbioportalProcessor(Processor):

    name = "cbioportal"

    def __init__(self, path: Union[None, str, Path] = None):
        if path is None:
            path = os.path.join(os.path.dirname(__file__), "data_CNA.txt")
        elif isinstance(path, str):
            path = Path(path)

        self.df = pd.read_csv(path, sep="\t")
        self.rel_type = "copy_number_altered_in"

    def get_nodes(self):
        for index, row in self.df.iterrows():
            hgnc_id = hgnc_client.get_hgnc_id(row["Hugo_Symbol"])
            if not hgnc_id:
                continue
            yield Node(db_ns="hgnc", db_id=hgnc_id, labels=["BioEntity"])

        for cell_line in self.df.columns.values[1:]:
            yield Node(db_ns="ccle", db_id=cell_line, labels=["BioEntity"])

    def get_relations(self):
        for index, row in self.df.iterrows():
            hgnc_id = hgnc_client.get_hgnc_id(row["Hugo_Symbol"])
            if not hgnc_id:
                continue
            for cell_line in self.df.columns.values[1:]:
                if row[cell_line] != 0:
                    yield Relation(
                        source_ns="hgnc",
                        source_id=hgnc_id,
                        target_ns="ccle",
                        target_id=cell_line,
                        rel_type=self.rel_type,
                        data={"CNA": row[cell_line]},
                    )
