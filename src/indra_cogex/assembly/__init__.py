# -*- coding: utf-8 -*-

"""Assembly of Node objects."""
import pystow
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional

from indra_cogex.representation import Node


ASSEMBLED_MODULE = pystow.module("indra", "cogex", "assembled")


class NodeAssembler:
    """Assembles Node objects."""

    def __init__(self, nodes: Optional[List[Node]] = None):
        """Initialize a new NodeAssembler object.

        Parameters
        ----------
        nodes :
            A list of Node objects.
        """
        self.nodes = nodes if nodes else []
        self.conflicts: List[Conflict] = []

    def add_nodes(self, nodes: List[Node]):
        """Add a list of Node objects to the assembler.

        Parameters
        ----------
        nodes :
            A list of Node objects.
        """
        self.nodes += nodes

    def assemble_nodes(self) -> List[Node]:
        """Assemble the nodes in the assembler.

        Nodes with the same grounding are assembled into a single node that
        contains all the labels and data from all the nodes.

        Returns
        -------
        nodes :
            A list of Node objects.
        """
        nodes_by_id = defaultdict(list)
        for node in self.nodes:
            nodes_by_id[(node.db_ns, node.db_id)].append(node)

        assembled_nodes = [
            self.get_aggregate_node(db_ns, db_id, node_group)
            for (db_ns, db_id), node_group in nodes_by_id.items()
        ]
        return assembled_nodes

    def get_aggregate_node(self, db_ns: str, db_id: str, nodes: List[Node]) -> Node:
        """Aggregate a list of Node objects.

        Parameters
        ----------
        db_ns :
            The database namespace of the nodes.
        db_id :
            The database id of the nodes.
        nodes :
            A list of Node objects.

        Returns
        -------
        :
            A Node object with all the
            labels and data from the input nodes.
        """
        labels = set()
        data: Dict[str, str] = {}
        for node in nodes:
            labels |= set(node.labels)
            for data_key, data_val in node.data.items():
                previous_val = data.get(data_key)
                if previous_val and previous_val != data_val:
                    self.conflicts.append(Conflict(data_key, previous_val, data_val))
                else:
                    data[data_key] = data_val
        return Node(db_ns, db_id, sorted(labels), data, validate_data=True)


class Conflict:
    def __init__(self, key, val1, val2):
        self.key = key
        self.val1 = val1
        self.val2 = val2

    def __repr__(self):
        return str(self)

    def __str__(self):
        return f"Conflict({self.key}, {self.val1}, {self.val2})"


def get_assembled_path(node_type: str) -> Path:
    nodes_path = ASSEMBLED_MODULE.join(name=f"nodes_{node_type}.tsv.gz")
    return nodes_path
