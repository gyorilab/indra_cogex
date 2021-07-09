import pandas as pd
import os

from pathlib import Path
from typing import Union

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
        for index, gene in self.df.iterrows():
            yield Node(db_ns="HGNC", db_id=gene["Hugo_Symbol"])

        for cell_line in self.df.columns.values[1:]:
            yield Node(db_ns="CCLE", db_id=cell_line)

    def get_relations(self):
        for index, gene in self.df.iterrows():
            for cell_line in self.df.columns.values[1:]:
                if gene[cell_line] != 0:
                    yield Relation(
                        source_ns="HGNC",
                        source_id=gene["Hugo_Symbol"],
                        target_ns="CCLE",
                        target_id=cell_line,
                        rel_type=self.rel_type,
                    )
