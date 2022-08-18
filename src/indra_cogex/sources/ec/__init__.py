# -*- coding: utf-8 -*-

"""Processor for enzyme codes."""

from typing import Iterable

from indra.databases import hgnc_client
from indra.databases.hgnc_client import enzyme_to_hgncs, hgnc_to_enzymes
from indra.ontology.bio import bio_ontology

from indra_cogex.representation import Node, Relation
from indra_cogex.sources import Processor

__all__ = [
    "HGNCEnzymeProcessor",
]


class HGNCEnzymeProcessor(Processor):
    """A processor for HGNC enzyme annotations."""

    name = "hgnc_enzymes"
    node_types = ["BioEntity"]

    def __init__(self):
        self.genes = {
            hgnc_id: Node.standardized(
                db_ns="hgnc",
                db_id=hgnc_id,
                name=hgnc_client.get_hgnc_name(hgnc_id),
                labels=["BioEntity"],
            )
            for hgnc_id in hgnc_to_enzymes
        }

        self.enzymes = {}
        for enzyme_id in enzyme_to_hgncs:
            stripped_id = _strip_ec_code(enzyme_id)
            self.enzymes[enzyme_id] = Node.standardized(
                db_ns="ec-code",
                db_id=stripped_id,
                name=bio_ontology.get_name("ECCODE", stripped_id),
                labels=["BioEntity"],
            )

    def get_nodes(self) -> Iterable[Node]:
        """Iterate over HGNC genes and enzymes."""
        yield from self.genes.values()
        yield from self.enzymes.values()

    def get_relations(self) -> Iterable[Relation]:
        """Iterate over HGNC's enzyme annotations."""
        for hgnc_id, enzyme_ids in hgnc_to_enzymes.items():
            for enzyme_id in enzyme_ids:
                gene = self.genes[hgnc_id]
                enzyme = self.enzymes[enzyme_id]
                yield Relation(
                    gene.db_ns,
                    gene.db_id,
                    enzyme.db_ns,
                    enzyme.db_id,
                    "has_activity",
                    dict(source=self.name),
                )


def _strip_ec_code(code: str) -> str:
    """Strips off trailing dashes from ec codes"""
    # Continue to strip off '.-' until name does not end with '.-'
    while code.endswith(".-"):
        code = code[:-2]
    return code


if __name__ == "__main__":
    HGNCEnzymeProcessor.cli()
