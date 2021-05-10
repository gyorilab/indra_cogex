# -*- coding: utf-8 -*-

"""Base classes for processors."""

import csv
import gzip
from abc import ABC
from operator import attrgetter
from pathlib import Path
from typing import ClassVar, Iterable

import click
import pystow
from more_click import verbose_option
from tqdm import tqdm

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
    module: ClassVar[pystow.Module]
    directory: ClassVar[Path]


    def __init_subclass__(cls, **kwargs):
        cls.module = pystow.module("indra", "cogex", cls.name)
        cls.directory = cls.module.base
        cls.nodes_path = cls.module.join(name="nodes.tsv.gz")
        cls.edges_path = cls.module.join(name="edges.tsv.gz")

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
        sample_path = self.module.join(name=f"nodes_sample.tsv")
        if self.nodes_path.is_file():
            return self.nodes_path

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

        with gzip.open(self.nodes_path, mode="wt") as node_file:
            node_writer = csv.writer(node_file, delimiter="\t")
            with sample_path.open("w") as node_sample_file:
                node_sample_writer = csv.writer(node_sample_file, delimiter="\t")

                header = f"id:ID", ":LABEL", *metadata
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

        return self.nodes_path

    def _dump_edges(self) -> Path:
        sample_path = self.module.join(name=f"edges_sample.tsv")
        if self.edges_path.is_file():
            return self.edges_path

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

        with gzip.open(self.edges_path, "wt") as edge_file:
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
        return self.edges_path
