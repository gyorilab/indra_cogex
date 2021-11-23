# -*- coding: utf-8 -*-

"""Run the sources CLI."""

import json
import os
import pickle
from pathlib import Path
from textwrap import dedent
from typing import Iterable, Optional, TextIO, Type

import click
import pystow
from more_click import verbose_option

from . import processor_resolver
from .processor import Processor
from ..assembly import NodeAssembler


def _iter_resolvers() -> Iterable[Type[Processor]]:
    return iter(processor_resolver)


@click.command()
@click.option(
    "--load",
    is_flag=True,
    help="If true, automatically loads the data through ``neo4j-admin import``",
)
@click.option(
    "--load_only",
    is_flag=True,
    help="If true, load the dumped data tables into neo4j without invoking sources",
)
@click.option(
    "-f",
    "--force",
    is_flag=True,
    help="If true, rebuild all resources",
)
@click.option(
    "--with_sudo",
    is_flag=True,
    help="If true, sudo is prepended to the neo4j-admin import command",
)
@click.option(
    "--config",
    type=click.File("r"),
    help="Path to a JSON configuration file whose keys match the names of the processors"
    " and values are dictionaries matching the __init__ parameters for the processor",
)
@click.option(
    "--allow-missing", is_flag=True, help="If true, doesn't explode on missing files"
)
@verbose_option
def main(
    load: bool,
    load_only: bool,
    force: bool,
    with_sudo: bool,
    config: Optional[TextIO],
    allow_missing: bool,
):
    """Generate and import Neo4j nodes and edges tables."""
    # Paths to files with preprocessed nodes (e.g. assembled nodes or nodes that don't need to be assembled)
    preprocessed_nodes_paths = []
    to_assemble = ["BioEntity", "Publication"]
    config = {} if config is None else json.load(config)
    paths = []
    node_assemblers = {}
    for processor_cls in _iter_resolvers():
        if not processor_cls.importable:
            continue
        click.secho(f"Checking {processor_cls.name}", fg="green", bold=True)
        if not load_only:
            if (
                force
                or not processor_cls.nodes_path.is_file()
                or not processor_cls.nodes_indra_path.is_file()
                or not processor_cls.edges_path.is_file()
            ):
                try:
                    processor = processor_cls(**config.get(processor_cls.name, {}))
                except FileNotFoundError as e:
                    if not allow_missing:
                        raise
                    click.secho(f"Failed: {e}", fg="red")
                    continue
                click.secho("Processing...", fg="green")
                # First dump the nodes and edges for processor
                paths_by_type, nodes_by_type, _ = processor.dump()
                # Assemble or store the paths to nodes depending on node type
                for node_type, nodes in nodes_by_type.items():
                    if node_type in to_assemble:
                        if node_type not in node_assemblers:
                            node_assemblers[node_type] = NodeAssembler()
                        node_assemblers[node_type].add_nodes(nodes)
                    else:
                        preprocessed_nodes_paths.append(paths_by_type[node_type])
            else:
                # Get paths to the nodes by type
                for node_type in processor_cls.nodes_types:
                    (
                        proc_nodes_path,
                        nodes_indra_path,
                        _,
                    ) = processor_cls._get_node_paths(node_type)
                    # Assemble or store the paths to nodes depending on node type
                    if node_type in to_assemble:
                        click.secho(
                            f"Loading cached nodes from {nodes_indra_path}",
                            fg="green",
                        )
                        with open(nodes_indra_path, "rb") as fh:
                            nodes = pickle.load(fh)
                            if node_type not in node_assemblers:
                                node_assemblers[node_type] = NodeAssembler()
                            node_assemblers[node_type].add_nodes(nodes)
                    else:
                        preprocessed_nodes_paths.append(proc_nodes_path)

        paths.append((processor_cls.nodes_path, processor_cls.edges_path))

    if not load_only:
        # if force or not nodes_path.is_file():  # Removing this since there are multiple nodes files
        if force:
            # Now create and dump the assembled nodes for each node type
            for node_type, na in node_assemblers.items():
                nodes_path = pystow.join(
                    "indra", "cogex", "assembled", name=f"nodes_{node_type}.tsv.gz"
                )
                nodes_path = Path(nodes_path)
                assembled_nodes = na.assemble_nodes()
                assembled_nodes = sorted(
                    assembled_nodes, key=lambda x: (x.db_ns, x.db_id)
                )
                Processor._dump_nodes_to_path(assembled_nodes, nodes_path)
                preprocessed_nodes_paths.append(nodes_path)

    if load or load_only:
        sudo_prefix = "" if not with_sudo else "sudo"
        command = dedent(
            f"""\
        {sudo_prefix} neo4j-admin import \\
          --database=indra \\
          --delimiter='TAB' \\
          --skip-duplicate-nodes=true \\
          --skip-bad-relationships=true
        """
        ).rstrip()
        for node_path in preprocessed_nodes_paths:
            command += f"\\\n --nodes {node_path}"
        for _, edge_path in paths:
            command += f"\\\n --relationships {edge_path}"

        click.secho("Running shell command:")
        click.secho(command, fg="blue")
        os.system(command)  # noqa:S605


if __name__ == "__main__":
    main()
