# -*- coding: utf-8 -*-

"""Base classes for processors."""

import csv
import gzip
from abc import ABC
from collections import defaultdict
from pathlib import Path
from typing import ClassVar, DefaultDict, Iterable, Set, Tuple

import pystow
from tqdm import tqdm

from indra_cogex.representation import Node, Relation

__all__ = [
    'Processor',
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
        return pystow.module('indra', 'cogex', self.name)

    def get_nodes(self) -> Iterable[Node]:
        """Iterate over the nodes to upload."""
        raise NotImplemented

    def get_relations(self) -> Iterable[Relation]:
        """Iterate over the relations to upload."""
        raise NotImplemented

    def dump(self) -> None:
        """Dump the contents of this processor to CSV files ready for use in ``neo4-admin import``."""
        id_to_type = {}

        type_to_node: DefaultDict[str, Set[Node]] = defaultdict(set)
        type_to_metadata = defaultdict(set)
        for node in self.get_nodes():
            ntype = id_to_type[node.identifier] = node.labels[0]
            type_to_node[ntype].add(node)
            type_to_metadata[ntype].update(node.data.keys())

        for ntype, nodes in type_to_node.items():
            metadata = sorted(type_to_metadata[ntype])

            data_sample_path = self.module.join('nodes', name=f'{ntype}_sample.csv')
            data_path = self.module.join('nodes', name=f'{ntype}.csv.gz')
            # cypher_path = self.module.join('nodes', name=f'{ntype}.cypher.txt')

            with gzip.open(data_path, mode='wt') as node_file:
                nodes = tqdm(nodes, desc=f'Node: {ntype}', unit_scale=True)
                node_rows = (
                    (node.identifier, ntype, *[node.data.get(key, '') for key in metadata])
                    for node in nodes
                )
                node_writer = csv.writer(node_file)

                with data_sample_path.open('w') as node_sample_file:
                    node_sample_writer = csv.writer(node_sample_file)

                    header = f'{ntype.lower()}Id:ID', ':LABEL', *metadata
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

        types_to_rel: DefaultDict[Tuple[str, str, str], Set[Relation]] = defaultdict(set)
        types_to_metadata: DefaultDict[Tuple[str, str, str], Set[Relation]] = defaultdict(set)
        for rel in self.get_relations():
            rel_type = rel.labels[0]
            t = id_to_type[rel.source_id], rel_type, id_to_type[rel.target_id]
            types_to_rel[t].add(rel)
            types_to_metadata[t].update(rel.data.keys())
        for (stype, rtype, ttype), rels in types_to_rel.items():
            metadata = sorted(type_to_metadata[stype, rtype, ttype])
            edge_data_path = self.module.join('edges', name=f'{stype}_{rtype}_{ttype}.csv.gz')
            edge_data_sample_path = self.module.join('edges', name=f'{stype}_{rtype}_{ttype}_sample.tsv')
            with gzip.open(edge_data_path, 'wt') as edge_file:
                rels = tqdm(rels, desc=f'Edge: {stype} {rtype} {ttype}', unit_scale=True)
                edge_rows = (
                    (rel.source_id, rel.target_id, rtype, *[rel.data.get(key) for key in metadata])
                    for rel in rels
                )
                edge_writer = csv.writer(edge_file)

                with edge_data_sample_path.open('w') as edge_sample_file:
                    edge_sample_writer = csv.writer(edge_sample_file)
                    header = ':START_ID', ':END_ID', ':TYPE', *metadata
                    edge_sample_writer.writerow(header)
                    edge_writer.writerow(header)

                    for _, edge_row in zip(range(10), edge_rows):
                        edge_sample_writer.writerow(edge_row)
                        edge_writer.writerow(edge_row)

                # Write remaining edges
                edge_writer.writerows(edge_rows)
