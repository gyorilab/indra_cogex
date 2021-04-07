import pickle
from indra_cogex.representation import Node, Relation


class Processor:
    def __init__(self, path):
        self.rel_type = 'expressed_in'
        with open(path, 'rb') as fh:
            self.expressions = pickle.load(fh)

    def get_nodes(self):
        for context_id in self.expressions:
            yield Node(context_id, ['BioEntity'])
        for hgnc_id in set.union({set(v) for v in self.expressions.values()}):
            yield Node(hgnc_id, ['BioEntity'])

    def get_relations(self):
        for context_id, hgnc_ids in self.expressions.items():
            for hgnc_id in hgnc_ids:
                yield Relation(hgnc_id, context_id, [self.rel_type])