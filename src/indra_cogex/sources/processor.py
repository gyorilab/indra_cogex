# -*- coding: utf-8 -*-

"""Base classes for processors."""

import csv
import gzip
from abc import ABC, abstractmethod
from operator import attrgetter
from pathlib import Path
from typing import ClassVar, Iterable

import click
import pystow
from more_click import verbose_option
from tqdm import tqdm

from indra.databases import identifiers

from indra_cogex.representation import Node, Relation

__all__ = [
    "Processor",
]


# deal with importing from wherever with
#  https://stackoverflow.com/questions/36922843/neo4j-3-x-load-csv-absolute-file-path
# Find neo4j conf and comment out this line: dbms.directories.import=import
# /usr/local/Cellar/neo4j/4.1.3/libexec/conf/neo4j.conf for me
# data stored in /usr/local/var/neo4j/data/databases


class Processor(ABC):
    """A processor creates nodes and iterables to upload to Neo4j."""

    name: ClassVar[str]
    module: ClassVar[pystow.Module]
    directory: ClassVar[Path]
    nodes_path: ClassVar[Path]
    edges_path: ClassVar[Path]
    importable = True

    def __init_subclass__(cls, **kwargs):
        """Initialize the class attributes."""
        cls.module = pystow.module("indra", "cogex", cls.name)
        cls.directory = cls.module.base
        cls.nodes_path = cls.module.join(name="nodes.tsv.gz")
        cls.edges_path = cls.module.join(name="edges.tsv.gz")

    @abstractmethod
    def get_nodes(self) -> Iterable[Node]:
        """Iterate over the nodes to upload."""

    @abstractmethod
    def get_relations(self) -> Iterable[Relation]:
        """Iterate over the relations to upload."""

    @classmethod
    def cli(cls):
        """Run the CLI for this processor."""

        @click.command()
        @verbose_option
        def _main():
            processor = cls()
            processor.dump()

        _main()

    def dump(self):
        """Dump the contents of this processor to CSV files ready for use in ``neo4-admin import``."""
        node_paths = self._dump_nodes()
        edge_paths = self._dump_edges()
        return node_paths, edge_paths

    def _dump_nodes(self) -> Path:
        sample_path = self.module.join(name="nodes_sample.tsv")
        if self.nodes_path.is_file():
            return self.nodes_path

        nodes = sorted(self.get_nodes(), key=lambda x: (x.db_ns, x.db_id))
        metadata = sorted(set(key for node in nodes for key in node.data))
        self._dump_nodes_to_path(nodes, metadata, self.nodes_path, sample_path)

    @staticmethod
    def _dump_nodes_to_path(nodes, metadata, nodes_path, sample_path=None):
        node_rows = (
            (
                norm_id(node.db_ns, node.db_id),
                "|".join(node.labels),
                *[node.data.get(key, "") for key in metadata],
            )
            for node in tqdm(nodes, desc="Nodes", unit_scale=True)
        )

        with gzip.open(nodes_path, mode="wt") as node_file:
            node_writer = csv.writer(node_file, delimiter="\t")  # type: ignore
            if sample_path:
                with sample_path.open("w") as node_sample_file:
                    node_sample_writer = csv.writer(node_sample_file, delimiter="\t")

                    header = "id:ID", ":LABEL", *metadata
                    node_sample_writer.writerow(header)
                    node_writer.writerow(header)

                    for _, node_row in zip(range(10), node_rows):
                        node_sample_writer.writerow(node_row)
                        node_writer.writerow(node_row)
            # Write remaining nodes
            node_writer.writerows(node_rows)

        return nodes_path

    def _dump_edges(self) -> Path:
        sample_path = self.module.join(name="edges_sample.tsv")
        if self.edges_path.is_file():
            return self.edges_path

        rels = self.get_relations()
        rels = sorted(
            rels, key=lambda r: (r.source_ns, r.source_id, r.target_ns, r.target_id)
        )
        metadata = sorted(set(key for rel in rels for key in rel.data))
        edge_rows = (
            (
                norm_id(rel.source_ns, rel.source_id),
                norm_id(rel.target_ns, rel.target_id),
                "|".join(sorted(rel.labels)),
                *[rel.data.get(key) for key in metadata],
            )
            for rel in tqdm(rels, desc="Edges", unit_scale=True)
        )

        with gzip.open(self.edges_path, "wt") as edge_file:
            edge_writer = csv.writer(edge_file, delimiter="\t")  # type: ignore
            with sample_path.open("w") as edge_sample_file:
                edge_sample_writer = csv.writer(edge_sample_file, delimiter="\t")
                header = ":START_ID", ":END_ID", ":TYPE", *metadata
                edge_sample_writer.writerow(header)
                edge_writer.writerow(header)

                for _, edge_row in zip(range(10), edge_rows):
                    edge_sample_writer.writerow(edge_row)
                    edge_writer.writerow(edge_row)

            # Write remaining edges
            edge_writer.writerows(edge_rows)
        return self.edges_path


def norm_id(db_ns, db_id):
    identifiers_ns = identifiers.get_identifiers_ns(db_ns)
    identifiers_id = db_id
    if not identifiers_ns:
        identifiers_ns = db_ns.lower()
    else:
        ns_embedded = identifiers.identifiers_registry.get(identifiers_ns, {}).get(
            "namespace_embedded"
        )
        if ns_embedded:
            identifiers_id = identifiers_id[len(identifiers_ns) + 1 :]
    return f"{identifiers_ns}:{identifiers_id}"
