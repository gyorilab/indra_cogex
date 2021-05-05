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
        for node, data in self.ontology.nodes(data=True):
            yield Node(_norm(node), ['BioEntity'], data)

    def get_relations(self):
        for source, target, data in self.ontology.edges(data=True):
            data = copy.copy(data)
            edge_type = data.pop('type')
            yield Relation(_norm(source), _norm(target), [edge_type], data)


def _norm(node: str) -> str:
    ns, identifier = node.split(':', 1)
    if identifier.startswith(f'{ns}:'):
        identifier = identifier[len(ns) + 1:]
    return f'{ns}:{identifier}'
