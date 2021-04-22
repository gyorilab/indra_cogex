# -*- coding: utf-8 -*-

"""Base classes for processors."""

from collections import defaultdict

import pystow
from abc import ABC
from pathlib import Path
from textwrap import dedent
from tqdm import tqdm
from typing import ClassVar, DefaultDict, Iterable, Set, Tuple

from indra_cogex.representation import Node, Relation

__all__ = [
    'Processor',
]

# deal with importing from wherever with https://stackoverflow.com/questions/36922843/neo4j-3-x-load-csv-absolute-file-path
# Find neo4j conf and comment out this line: dbms.directories.import=import
# /usr/local/Cellar/neo4j/4.1.3/libexec/conf/neo4j.conf for me


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
        return pystow.join('indra', 'cogex', self.name)

    def get_nodes(self) -> Iterable[Node]:
        """Iterate over the nodes to upload."""
        raise NotImplemented

    def get_relations(self) -> Iterable[Relation]:
        """Iterate over the relations to upload."""
        raise NotImplemented

    def dump(self):
        id_to_type = {}

        type_to_node: DefaultDict[str, Set[Node]] = defaultdict(set)
        type_to_metadata = defaultdict(set)
        for node in self.get_nodes():
            ntype = id_to_type[node.identifier] = node.labels[0]
            type_to_node[ntype].add(node)
            type_to_metadata[ntype].update(node.data.keys())

        pmodule = pystow.module('indra', 'cogex', self.name)
        for ntype, nodes in type_to_node.items():
            metadata = sorted(type_to_metadata[ntype])
            data_path = pmodule.join('nodes', name=f'{ntype}.tsv')
            cypher_path = pmodule.join('nodes', name=f'{ntype}.cypher.txt')
            print(ntype, metadata, len(nodes))
            with data_path.open('w') as file:
                print('identifier', *metadata, sep='\t', file=file)
                for node in tqdm(nodes, desc=f'Node: {ntype}', unit_scale=True):
                    print(node.identifier, *[node.data.get(key, '') for key in metadata], file=file, sep='\t')
            cypher = dedent(f'''\
                CREATE CONSTRAINT ON (n:{ntype}) ASSERT n.id IS UNIQUE;
                USING PERIODIC COMMIT
                LOAD CSV WITH HEADERS FROM "file://{data_path.as_posix()}" AS row FIELDTERMINATOR '\\t'
                MERGE (n:{ntype} {{ id: row.identifier }})
                ''')
            if metadata:
                creates = '\n'.join(
                    f'n.{key} = row.{key}'
                    for key in metadata
                )
                cypher += f'ON CREATE SET {creates}'
            with cypher_path.open('w') as file:
                print(cypher, file=file)

        types_to_rel: DefaultDict[Tuple[str, str, str], Set[Relation]] = defaultdict(set)
        types_to_metadata: DefaultDict[Tuple[str, str, str], Set[Relation]] = defaultdict(set)
        for rel in self.get_relations():
            rel_type = rel.labels[0]
            t = id_to_type[rel.source_id], rel_type, id_to_type[rel.target_id]
            types_to_rel[t].add(rel)
            types_to_metadata[t].update(rel.data.keys())
        for (stype, rtype, ttype), rels in types_to_rel.items():
            metadata = sorted(type_to_metadata[stype, rtype, ttype])
            data_path = pmodule.join('edges', name=f'{stype}_{rtype}_{ttype}.tsv')
            with data_path.open('w') as file:
                print('source_id', 'target_id', *metadata, sep='\t', file=file)
                for rel in tqdm(rels, desc=f'Edge: {stype} {rtype} {ttype}', unit_scale=True):
                    print(rel.source_id, rel.target_id, *[rel.data.get(key) for key in metadata], file=file, sep='\t')
