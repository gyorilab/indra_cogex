# -*- coding: utf-8 -*-

"""Representations for nodes and relations to upload to Neo4j."""

from typing import Any, Collection, Mapping, Optional

__all__ = ["Node", "Relation"]


class Node:
    """Representation for a node."""

    def __init__(
        self,
        identifier: str,
        labels: Collection[str],
        data: Optional[Mapping[str, Any]] = None,
    ):
        """Initialize the node.

        :param identifier: The identifier of the node
        :param labels: The collection of labels for the relation.
        :param data: The optional data dictionary associated with the node.
        """
        self.identifier = identifier
        self.labels = labels
        self.data = data if data else {}

    def to_json(self):
        """Serialize the node to JSON."""
        data = {k: v for k, v in self.data.items()}
        data["id"] = self.identifier
        return {"labels": self.labels, "data": data}

    def _get_data_str(self):
        pieces = ["id:'%s'" % self.identifier]
        for k, v in self.data.items():
            if isinstance(v, str):
                value = "'" + v.replace("'", "\\'") + "'"
            elif isinstance(v, (bool, int, float)):
                value = v
            else:
                value = str(v)
            piece = "%s:%s" % (k, value)
            pieces.append(piece)
        data_str = ", ".join(pieces)
        return data_str

    def __str__(self):  # noqa:D105
        data_str = self._get_data_str()
        labels_str = ":".join(self.labels)
        return f"(:{labels_str} {{ {data_str} }})"

    def __repr__(self):  # noqa:D105
        return str(self)


class Relation:
    """Representation for a relation."""

    def __init__(
        self,
        source_id: str,
        target_id: str,
        labels: Collection[str],
        data: Optional[Mapping[str, Any]] = None,
    ):
        """Initialize the relation.

        :param source_id: The identifier of the source node
        :param target_id: The identifier of the target node
        :param labels: The collection of labels for the relation.
        :param data: The optional data dictionary associated with the relation.
        """
        self.source_id = source_id
        self.target_id = target_id
        self.labels = list(labels)
        self.data = data if data else {}

    def to_json(self):
        """Serialize the relation to JSON."""
        return {
            "source_id": self.source_id,
            "target_id": self.target_id,
            "labels": self.labels,
            "data": self.data,
        }

    def __str__(self):  # noqa:D105
        data_str = ", ".join(["%s:'%s'" % (k, v) for k, v in self.data.items()])
        labels_str = ":".join(self.labels)
        return f"({self.source_id})-[:{labels_str} {data_str}]->" f"({self.target_id})"

    def __repr__(self):  # noqa:D105
        return str(self)
