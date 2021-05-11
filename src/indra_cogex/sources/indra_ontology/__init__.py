# -*- coding: utf-8 -*-

"""Processor for the INDRA ontology."""

import copy
import logging
from typing import Optional

from indra.ontology import IndraOntology
from indra_cogex.representation import Node, Relation
from indra_cogex.sources.processor import Processor

logger = logging.getLogger(__name__)


class OntologyProcessor(Processor):
    """Processor for the INDRA ontology."""

    name = "ontology"
    ontology: IndraOntology

    def __init__(self, ontology: Optional[IndraOntology] = None):
        """Initialize the INDRA ontology processor.

        :param ontology: An instance of an INDRA ontology. If none, loads the INDRA bio_ontology.
        """
        if ontology is None:
            import indra.ontology.bio

            self.ontology = indra.ontology.bio.bio_ontology
        else:
            self.ontology = ontology
        self.ontology.initialize()

    def get_nodes(self):  # noqa:D102
        for node, data in self.ontology.nodes(data=True):
            yield Node(_norm(node), ["BioEntity"], data)

    def get_relations(self):  # noqa:D102
        for source, target, data in self.ontology.edges(data=True):
            data = copy.copy(data)
            edge_type = data.pop("type")
            yield Relation(_norm(source), _norm(target), [edge_type], data)


def _norm(node: str) -> str:
    ns, identifier = node.split(":", 1)
    if identifier.startswith(f"{ns}:"):
        identifier = identifier[len(ns) + 1 :]
    return f"{ns}:{identifier}"
