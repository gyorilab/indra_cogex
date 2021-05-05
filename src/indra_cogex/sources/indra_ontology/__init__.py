import copy
import logging
from typing import Optional

from indra.ontology import IndraOntology
from indra_cogex.representation import Node, Relation
from indra_cogex.sources.processor import Processor

logger = logging.getLogger(__name__)


class OntologyProcessor(Processor):
    name = 'ontology'
    ontology: IndraOntology

    def __init__(self, ontology: Optional[IndraOntology] = None):
        if ontology is None:
            from indra.ontology.bio import bio_ontology as ontology
        self.ontology = ontology
        self.ontology.initialize()

    def get_nodes(self):
        for node in self.ontology:
            # ns, id = pyobo.normalize_curie(node)
            # if not ns or not id:
            #     logger.warning('could not normalize %s', node)
            #     continue
            # node = f'{ns}:{id}'
            yield Node(node, ['BioEntity'], self.ontology.nodes[node])

    def get_relations(self):
        for source, target, data in self.ontology.edges(data=True):
            data = copy.copy(data)
            edge_type = data.pop('type')
            yield Relation(source, target, [edge_type], data)
