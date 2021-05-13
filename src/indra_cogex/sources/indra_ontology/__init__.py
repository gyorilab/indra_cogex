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
            db_ns, db_id = self.ontology.get_ns_id(node)
            yield Node(db_ns, db_id, ["BioEntity"], data)

    def get_relations(self):  # noqa:D102
        for source, target, data in self.ontology.edges(data=True):
            source_ns, source_id = self.ontology.get_ns_id(source)
            target_ns, target_id = self.ontology.get_ns_id(target)
            data = copy.copy(data)
            edge_type = data.pop("type")
            yield Relation(
                source_ns, source_id, target_ns, target_id, [edge_type], data
            )
