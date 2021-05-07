import os
import pickle
import pyobo

from indra_cogex.representation import Node, Relation
from indra_cogex.sources.processor import Processor


class BgeeProcessor(Processor):
    name = "bgee"

    def __init__(self, path=None):
        if path is None:
            path = os.path.join(os.path.dirname(__file__), "expressions.pkl")
        self.rel_type = "expressed_in"
        with open(path, "rb") as fh:
            self.expressions = pickle.load(fh)

    def get_nodes(self):
        for context_id in self.expressions:
            yield Node(
                context_id,
                ["BioEntity"],
                data={"name": pyobo.get_name_by_curie(context_id)},
            )
        for hgnc_id in set.union(*[set(v) for v in self.expressions.values()]):
            yield Node(
                f"HGNC:{hgnc_id}",
                ["BioEntity"],
                data={"name": pyobo.get_name("hgnc", hgnc_id)},
            )

    def get_relations(self):
        for context_id, hgnc_ids in self.expressions.items():
            for hgnc_id in hgnc_ids:
                yield Relation(f"HGNC:{hgnc_id}", context_id, [self.rel_type])
