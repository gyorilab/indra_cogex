from collections import defaultdict
from typing import List
from indra_cogex.representation import Node


class NodeAssembler:
    def __init__(self, nodes: List[Node]):
        self.nodes = nodes
        self.conflicts = []

    def add_nodes(self, nodes: List[Node]):
        self.nodes += nodes

    def assemble_nodes(self) -> List[Node]:
        nodes_by_id = defaultdict(list)
        for node in self.nodes:
            nodes_by_id[node.identifier].append(node)

        assembled_nodes = [
            self.get_aggregate_node(identifier, node_group)
            for identifier, node_group in nodes_by_id.items()
        ]
        return assembled_nodes

    def get_aggregate_node(self, identifier: str, nodes: List[Node]) -> Node:
        labels = set()
        data = {}
        for node in nodes:
            labels |= node.labels
            for data_key, data_val in node.data.items():
                previous_val = data.get(data_key)
                if previous_val and previous_val != data_val:
                    self.conflicts.append(
                        Conflict(f"{data_key}:{previous_val}"), f"{data_key}:{data_val}"
                    )
                else:
                    data[data_key] = data_val
        return Node(identifier, labels, data)


class Conflict:
    def __init__(self, first, second):
        self.first = first
        self.second = second

    def __repr__(self):
        return str(self)

    def __str__(self):
        return f"Conflict({self.first}, {self.second})"
