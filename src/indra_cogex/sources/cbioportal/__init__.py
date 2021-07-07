import pandas as pd

from pathlib import Path
from typing import Union

from indra_cogex.representation import Node, Relation
from indra_cogex.sources.processor import Processor


class CbioportalProcessor(Processor):

    name = "cbioportal"

    def __init__(self, path):
        self.table = pd.read_csv(path, sep="\t")

    # TODO: def get_nodes(self):

    # TODO: def get_relations(self):
