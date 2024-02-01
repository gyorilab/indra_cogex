# -*- coding: utf-8 -*-

"""Processor for the INDRA ontology."""

import copy
import logging
from typing import Optional, Any, Mapping

from indra.ontology import IndraOntology
from indra_cogex.representation import Node, Relation
from indra_cogex.sources.processor import Processor
from indra_cogex.sources.utils import get_bool

logger = logging.getLogger(__name__)


class OntologyProcessor(Processor):
    """Processor for the INDRA ontology."""

    name = "ontology"
    ontology: IndraOntology
    node_types = ["BioEntity"]

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
            parsed_data = _get_data(data)
            yield Node(db_ns, db_id, ["BioEntity"], data=parsed_data)

    def get_relations(self):  # noqa:D102
        for source, target, data in self.ontology.edges(data=True):
            source_ns, source_id = self.ontology.get_ns_id(source)
            target_ns, target_id = self.ontology.get_ns_id(target)
            data = copy.copy(data)
            edge_type = data.pop("type")
            yield Relation(
                source_ns, source_id, target_ns, target_id, edge_type, data
            )


def _get_data(data: Mapping[str, Any]) -> Mapping[str, Any]:
    """Make sure the data has the proper keys for Neo4j headers."""
    out = {}
    for key, value in data.items():
        if isinstance(value, bool):
            new_key = key + ":boolean"
            out[new_key] = get_bool(value)
        else:
            # TODO: handle other types - this has to make sure all other
            #  sources that assemble BioEntity nodes use the same types
            out[key] = value
    return out
