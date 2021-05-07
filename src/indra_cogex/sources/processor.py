# -*- coding: utf-8 -*-

"""Base classes for processors."""

import gzip

import click
import csv
import pystow
from abc import ABC
from more_click import verbose_option
from operator import attrgetter
from pathlib import Path
from tqdm import tqdm
from typing import ClassVar, Iterable

from indra_cogex.representation import Node, Relation

__all__ = [
    "Processor",
]

# deal with importing from wherever with https://stackoverflow.com/questions/36922843/neo4j-3-x-load-csv-absolute-file-path
# Find neo4j conf and comment out this line: dbms.directories.import=import
# /usr/local/Cellar/neo4j/4.1.3/libexec/conf/neo4j.conf for me
# data stored in /usr/local/var/neo4j/data/databases

"""
1. Iterate all nodes
2. group by node type
2. Metadata configuration
3. Dump to TSV
4. Bulk load

1. Iterate all relations
2. Group by source/target node type
3. Output different TSV for each as well as metadata TSV to automatically build bulk load query
"""


class Processor(ABC):
    """A processor creates nodes and iterables to upload to Neo4j."""

    name: ClassVar[str]

    @property
    def directory(self) -> Path:
        """Return the directory for this processor."""
        return self.module.base

    @property
    def module(self) -> pystow.Module:
        """Return the :mod:`pystow` module for this processor."""
        return pystow.module("indra", "cogex", self.name)

    def get_nodes(self) -> Iterable[Node]:
        """Iterate over the nodes to upload."""
        raise NotImplemented

    def get_relations(self) -> Iterable[Relation]:
        """Iterate over the relations to upload."""
        raise NotImplemented

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
        path = self.module.join(name=f"nodes.tsv.gz")
        sample_path = self.module.join(name=f"nodes_sample.tsv")
        if path.is_file():
            return path

        nodes = sorted(self.get_nodes(), key=attrgetter("identifier"))
        metadata = sorted(set(key for node in nodes for key in node.data))
        node_rows = (
            (
                node.identifier,
                "|".join(node.labels),
                *[node.data.get(key, "") for key in metadata],
            )
            for node in tqdm(nodes, desc=f"Nodes", unit_scale=True)
        )

        with gzip.open(path, mode="wt") as node_file:
            node_writer = csv.writer(node_file, delimiter="\t")
            with sample_path.open("w") as node_sample_file:
                node_sample_writer = csv.writer(node_sample_file, delimiter="\t")

                header = f":ID", ":LABEL", *metadata
                node_sample_writer.writerow(header)
                node_writer.writerow(header)

                for _, node_row in zip(range(10), node_rows):
                    node_sample_writer.writerow(node_row)
                    node_writer.writerow(node_row)

            # Write remaining nodes
            node_writer.writerows(node_rows)

            # cypher = dedent(f'''\
            #     CREATE CONSTRAINT ON (n:{ntype}) ASSERT n.id IS UNIQUE;
            #     USING PERIODIC COMMIT
            #     LOAD CSV WITH HEADERS FROM "file://{data_path.as_posix()}" AS row FIELDTERMINATOR '\\t'
            #     MERGE (n:{ntype} {{ id: row.identifier }})
            #     ''')
            # if metadata:
            #     creates = '\n'.join(
            #         f'n.{key} = row.{key}'
            #         for key in metadata
            #     )
            #     cypher += f'ON CREATE SET {creates}'
            # with cypher_path.open('w') as file:
            #     print(cypher, file=file)

        return path

    def _dump_edges(self) -> Path:
        path = self.module.join(name=f"edges.tsv.gz")
        sample_path = self.module.join(name=f"edges_sample.tsv")
        if path.is_file():
            return path

        rels = self.get_relations()
        rels = sorted(rels, key=lambda r: (r.source_id, r.target_id))
        metadata = sorted(set(key for rel in rels for key in rel.data))
        edge_rows = (
            (
                rel.source_id,
                rel.target_id,
                "|".join(sorted(rel.labels)),
                *[rel.data.get(key) for key in metadata],
            )
            for rel in tqdm(rels, desc="Edges", unit_scale=True)
        )

        with gzip.open(path, "wt") as edge_file:
            edge_writer = csv.writer(edge_file, delimiter="\t")
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
        return path
