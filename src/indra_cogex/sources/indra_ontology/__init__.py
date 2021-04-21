import copy
from indra.ontology.bio import bio_ontology, BioOntology
from indra_cogex.representation import Node, Relation
from indra_cogex.sources.processor import Processor


class OntologyProcessor(Processor):
    def __init__(self):
        self.bio_ontology = bio_ontology
        self.bio_ontology.initialize()

    def get_nodes(self):
        for node in self.bio_ontology:
            yield Node(node, ['BioEntity'], self.bio_ontology.nodes[node])

    def get_relations(self):
        for source, target, data in self.bio_ontology.edges(data=True):
            data = copy.copy(data)
            edge_type = data.pop('type')
            yield Relation(source, target, [edge_type], data)
