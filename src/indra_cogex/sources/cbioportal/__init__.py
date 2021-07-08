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

    def get_nodes(self):
        for index, row in self.df.iterrows():
            yield Node(row['Hugo_Symbol'])

        for column in list(self.df.columns.values)[1:]:
            yield Node(column)

    # TODO: def get_relations(self):
