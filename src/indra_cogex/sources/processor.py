# -*- coding: utf-8 -*-

"""Base classes for processors."""

import csv
import gzip
import json
import logging
import pickle
from abc import ABC, abstractmethod
from collections import defaultdict
from pathlib import Path
from typing import ClassVar, Iterable, List, Tuple, Optional, Mapping, Any

import click
import pystow
from more_click import verbose_option
from tqdm import tqdm

from indra.statements.validate import assert_valid_db_refs, assert_valid_evidence
from indra.statements import Evidence

from indra_cogex.representation import Node, Relation, norm_id
from indra_cogex.sources.processor_util import (
    NEO4J_DATA_TYPES,
    data_validator,
    DataTypeError,
    UnknownTypeError,
)

__all__ = [
    "Processor",
]

logger = logging.getLogger(__name__)

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
    nodes_indra_path: ClassVar[Path]
    edges_path: ClassVar[Path]
    importable = True
    node_types = ClassVar[Iterable[str]]

    def __init_subclass__(cls, **kwargs):
        """Initialize the class attributes."""
        cls.module = pystow.module("indra", "cogex", cls.name)
        cls.directory = cls.module.base
        # These are nodes directly in the neo4j encoding
        cls.nodes_path = cls.module.join(name="nodes.tsv.gz")
        # These are nodes in the original INDRA-oriented representation
        # needed for assembly
        cls.nodes_indra_path = cls.module.join(name="nodes.pkl")
        cls.edges_path = cls.module.join(name="edges.tsv.gz")

    @abstractmethod
    def get_nodes(self) -> Iterable[Node]:
        """Iterate over the nodes to upload."""

    @abstractmethod
    def get_relations(self) -> Iterable[Relation]:
        """Iterate over the relations to upload."""

    @classmethod
    def get_cli(cls) -> click.Command:
        """Get the CLI for this processor."""

        @click.command()
        @verbose_option
        def _main():
            click.secho(f"Building {cls.name}", fg="green", bold=True)
            processor = cls()
            processor.dump()

        return _main

    @classmethod
    def cli(cls) -> None:
        """Run the CLI for this processor."""
        cls.get_cli()()

    def dump(self) -> Tuple[Path, List[Node], Path]:
        """Dump the contents of this processor to CSV files ready for use in ``neo4-admin import``."""
        node_paths, nodes = self._dump_nodes()
        edge_paths = self._dump_edges()
        return node_paths, nodes, edge_paths

    @classmethod
    def _get_node_paths(cls, node_type: str) -> Tuple[Path, Path, Path]:
        # If the processor returns multiple types of nodes, add node_type to the file name
        if len(cls.node_types) > 1:
            return (
                cls.module.join(name=f"nodes_{node_type}.tsv.gz"),
                cls.module.join(name=f"nodes_{node_type}.pkl"),
                cls.module.join(name=f"nodes_{node_type}_sample.tsv"),
            )
        return (
            cls.nodes_path,
            cls.nodes_indra_path,
            cls.module.join(name="nodes_sample.tsv"),
        )

    def _dump_nodes(self) -> Tuple[Path, List[Node]]:
        paths_by_type = {}
        nodes_by_type = defaultdict(list)
        # Get all the nodes
        nodes = tqdm(
            self.get_nodes(),
            desc="Node generation",
            unit_scale=True,
            unit="node",
        )
        # Map the nodes to their types
        for node in nodes:
            nodes_by_type[node.labels[0]].append(node)
        # Get the paths for each type of node and dump the nodes
        for node_type in nodes_by_type:
            nodes_path, nodes_indra_path, sample_path = self._get_node_paths(node_type)
            nodes = sorted(nodes_by_type[node_type], key=lambda x: (x.db_ns, x.db_id))
            with open(nodes_indra_path, "wb") as fh:
                pickle.dump(nodes, fh)
            self._dump_nodes_to_path(nodes, nodes_path, sample_path)
            paths_by_type[node_type] = nodes_path
        return paths_by_type, dict(nodes_by_type)

    def _dump_nodes_to_path(self, nodes, nodes_path, sample_path=None, write_mode="wt"):
        return self._dump_nodes_to_path_static(
            self.name,
            nodes,
            nodes_path,
            sample_path=sample_path,
            write_mode=write_mode,
        )

    @staticmethod
    def _dump_nodes_to_path_static(
        processor_name,
        nodes,
        nodes_path,
        sample_path=None,
        write_mode="wt"
    ):
        # This method is static so it can be used in the node assembly process
        # when running `python -m indra_cogex.sources` without instantiating
        # the processor used (some processors load their data on
        # instantiation and this needs to be avoided in the node assembly
        # proces)
        logger.info(f"Dumping into {nodes_path}...")
        try:
            nodes = list(validate_nodes(nodes))
        except (UnknownTypeError, DataTypeError) as e:
            logger.error(f"Bad node data type in node data values for {self.name}")
            raise e
        metadata = sorted(set(key for node in nodes for key in node.data))
        try:
            validate_headers(metadata)
        except TypeError as e:
            logger.error(f"Bad node data type in header for {processor_name}")
            raise e

        node_rows = (
            (
                norm_id(node.db_ns, node.db_id),
                ";".join(node.labels),
                *[node.data.get(key, "") for key in metadata],
            )
            for node in tqdm(nodes, desc="Node serialization", unit_scale=True)
        )

        header = "id:ID", ":LABEL", *metadata
        with gzip.open(nodes_path, mode=write_mode) as node_file:
            node_writer = csv.writer(node_file, delimiter="\t")  # type: ignore
            # Only add header when writing to a new file
            if write_mode == "wt":
                node_writer.writerow(header)
            if sample_path:
                with sample_path.open("w") as node_sample_file:
                    node_sample_writer = csv.writer(node_sample_file, delimiter="\t")
                    node_sample_writer.writerow(header)
                    for _, node_row in zip(range(10), node_rows):
                        node_sample_writer.writerow(node_row)
                        node_writer.writerow(node_row)
            # Write remaining nodes
            node_writer.writerows(node_rows)

        return nodes_path

    def _dump_edges(self) -> Path:
        sample_path = self.module.join(name="edges_sample.tsv")
        rels = self.get_relations()
        return self._dump_edges_to_path(rels, self.edges_path, sample_path)

    def _dump_edges_to_path(self, rels, edges_path, sample_path=None, write_mode="wt"):
        logger.info(f"Dumping into {edges_path}...")
        try:
            rels = validate_relations(rels)
        except (UnknownTypeError, DataTypeError) as e:
            logger.error(f"Bad edge data type in edge data values for {self.name}")
            raise e
        rels = sorted(
            rels, key=lambda r: (r.source_ns, r.source_id, r.target_ns, r.target_id)
        )
        metadata = sorted(set(key for rel in rels for key in rel.data))
        try:
            validate_headers(metadata)
        except TypeError as e:
            logger.error(f"Bad edge data type in header for {self.name}")
            raise e
        edge_rows = (
            (
                norm_id(rel.source_ns, rel.source_id),
                norm_id(rel.target_ns, rel.target_id),
                rel.rel_type,
                *[rel.data.get(key) for key in metadata],
            )
            for rel in tqdm(rels, desc="Edges", unit_scale=True)
        )

        with gzip.open(self.edges_path, mode=write_mode) as edge_file:
            edge_writer = csv.writer(edge_file, delimiter="\t")  # type: ignore
            header = ":START_ID", ":END_ID", ":TYPE", *metadata
            # Only add header when writing to a new file
            if write_mode == "wt":
                edge_writer.writerow(header)
            if sample_path:
                with sample_path.open("w") as edge_sample_file:
                    edge_sample_writer = csv.writer(edge_sample_file, delimiter="\t")
                    edge_sample_writer.writerow(header)
                    for _, edge_row in zip(range(10), edge_rows):
                        edge_sample_writer.writerow(edge_row)
                        edge_writer.writerow(edge_row)
            # Write remaining edges
            edge_writer.writerows(edge_rows)
        return edges_path


def assert_valid_node(
    db_ns: str,
    db_id: str,
    data: Optional[Mapping[str, Any]] = None,
    check_data: bool = False,
) -> None:
    if db_ns == "indra_evidence":
        if data and data.get("evidence:string"):
            ev = Evidence._from_json(json.loads(data["evidence"]))
            assert_valid_evidence(ev)
    else:
        assert_valid_db_refs({db_ns: db_id})

    if data and check_data:
        for key, value in data.items():
            if key == "evidence":
                continue
            if ":" in key:
                dtype = key.split(":")[1]
            else:
                # If no data type is specified, string is assumed by Neo4j
                dtype = "string"
            data_validator(dtype, value)


def validate_nodes(nodes: Iterable[Node]) -> Iterable[Node]:
    for idx, node in enumerate(nodes):
        check_data = idx < 10
        try:
            assert_valid_node(node.db_ns, node.db_id, node.data, check_data)
            yield node
        except (UnknownTypeError, DataTypeError) as e:
            logger.error(f"{idx}: {node} - {e}")
            logger.error("Bad node data type(s) detected")
            raise e
        except Exception as e:
            logger.info(f"{idx}: {node} - {e}")
            continue


def validate_relations(relations: Iterable[Relation]) -> Iterable[Relation]:
    for idx, rel in enumerate(relations):
        try:
            check_data = idx < 10
            assert_valid_node(rel.source_ns, rel.source_id, rel.data, check_data)
            assert_valid_node(rel.target_ns, rel.target_id)
            yield rel
        except (UnknownTypeError, DataTypeError) as e:
            logger.error(f"{idx}: {rel} - {e}")
            logger.error("Bad relation data type(s) detected")
            raise e
        except Exception as e:
            logger.info(f"{idx}: {rel} - {e}")
            continue


def validate_headers(headers: Iterable[str]) -> None:
    """Check for data types in the headers"""
    for header in headers:
        # If : is in the header and there is something after it check if
        # it's a valid data type
        if ":" in header and header.split(":")[1]:
            dtype = header.split(":")[1]

            # Strip trailing '[]' for array types
            if dtype.endswith("[]"):
                dtype = dtype[:-2]

            if dtype not in NEO4J_DATA_TYPES:
                raise TypeError(
                    f"Invalid header data type '{dtype}' for header {header}"
                )
