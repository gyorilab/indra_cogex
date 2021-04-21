from abc import ABC
from typing import Iterable

from indra_cogex.representation import Node, Relation

__all__ = [
    'Processor',
]


class Processor(ABC):
    """A processor creates nodes and iterables to upload to Neo4j."""

    def get_nodes(self) -> Iterable[Node]:
        """Iterate over the nodes to upload."""
        raise NotImplemented

    def get_relations(self) -> Iterable[Relation]:
        """Iterate over the relations to upload."""
        raise NotImplemented
