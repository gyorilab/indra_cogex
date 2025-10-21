# -*- coding: utf-8 -*-

"""Run the sources CLI."""

import json
import os
import pickle
from collections import defaultdict
from pathlib import Path
from textwrap import dedent
from typing import Iterable, Optional, TextIO, Type

import click
from more_click import verbose_option

from . import processor_resolver
from .processor import Processor
from ..assembly import NodeAssembler, get_assembled_path


def _iter_processors() -> Iterable[Type[Processor]]:
    return iter(processor_resolver)


@click.command()
@click.option(
    "--process",
    is_flag=True,
    help="If true, builds all missing resources.",
)
@click.option(
    "--force_process",
    is_flag=True,
    help="If true, rebuilds all resources",
)
@click.option(
    "--assemble",
    is_flag=True,
    help="If true, assembles all (not yet assembled) nodes.",
)
@click.option(
    "--force_assemble",
    is_flag=True,
    help="If true, reassembles all nodes.",
)
@click.option(
    "--run_import",
    is_flag=True,
    help="If true, automatically loads the data through ``neo4j-admin import``",
)
@click.option(
    "--force_import",
    is_flag=True,
    help="If true, forces the import even if the database already exists. This "
         "sets the --force flag of neo4j-admin import.",
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
    "--skip_failed_processors",
    is_flag=True,
    help="If true, skips processors that are missing required input files without erroring.",
)
@verbose_option
def main(
    process: bool,
    force_process: bool,
    assemble: bool,
    force_assemble: bool,
    run_import: bool,
    force_import: bool,
    with_sudo: bool,
    config: Optional[TextIO],
    skip_failed_processors: bool,
):
    """Generate and import Neo4j nodes and edges tables."""
    # Check which nodes labels need to be assembled (i.e. have multiple
    # processors)
    node_labels_to_processor_name = defaultdict(list)
    for processor_cls in _iter_processors():
        if not processor_cls.importable:
            continue
        for label in processor_cls.node_types:
            node_labels_to_processor_name[label].append(processor_cls.name)

    to_assemble = []
    for label, processor_names in node_labels_to_processor_name.items():
        if len(processor_names) > 1:
            to_assemble.append(label)
    click.secho(f"Node labels to assemble: {to_assemble}", fg="blue")

    # Paths to files with preprocessed nodes (e.g. assembled nodes or nodes that don't need to be assembled)
    nodes_paths_for_import = []
    config = {} if config is None else json.load(config)
    edge_paths = []
    node_assemblers = {}
    for processor_cls in _iter_processors():
        if not processor_cls.importable:
            continue
        click.secho(f"Checking {processor_cls.name}", fg="green", bold=True)
        # First, get all required paths, we'll need them in the next steps
        processed = True
        processor_import_paths = []
        processor_to_assemble_paths = {}
        for node_type in processor_cls.node_types:
            (
                proc_nodes_path,
                nodes_indra_path,
                _,
            ) = processor_cls._get_node_paths(node_type)
            if node_type in to_assemble:
                # Store the INDRA nodes pickle path for assembly
                processor_to_assemble_paths[node_type] = nodes_indra_path
            else:
                # These will be imported directly
                nodes_paths_for_import.append(proc_nodes_path)
                processor_import_paths.append(proc_nodes_path)
            if not proc_nodes_path.exists() or (
                nodes_indra_path and not nodes_indra_path.exists()
            ):
                processed = False
        if not processor_cls.edges_path.exists():
            processed = False
        edge_paths.append(processor_cls.edges_path)
        click.secho(
            f"Identified node paths for assembly: {[str(p) for p in processor_to_assemble_paths.values()]}",
            fg="blue",
        )
        click.secho(
            f"Identified node paths for import: {[str(p) for p in processor_import_paths]}",
            fg="blue",
        )
        # Run the processor if needed
        if force_process or (process and not processed):
            try:
                processor = processor_cls(**config.get(processor_cls.name, {}))
            except FileNotFoundError as e:
                if not skip_failed_processors:
                    raise
                click.secho(
                    f"Failed: {e}, skipping corresponding nodes and relations from further processing and import",
                    fg="red",
                )
                # Remove this processor's paths from the list of nodes/edges to import
                for path in processor_import_paths:
                    nodes_paths_for_import.remove(path)
                edge_paths.pop()
                continue
            click.secho("Processing...", fg="green")
            # First dump the nodes and edges for processor
            _, nodes_by_type, _ = processor.dump()
            # Add nodes to assembly if needed
            for node_type, nodes in nodes_by_type.items():
                if node_type in to_assemble:
                    assembled_path = get_assembled_path(node_type)
                    if force_assemble or (assemble and not assembled_path.exists()):
                        # Instantiate the assembler or add nodes to existing assembler
                        if node_type not in node_assemblers:
                            node_assemblers[node_type] = NodeAssembler()
                        node_assemblers[node_type].add_nodes(nodes)
        elif processed:  # force_process=False, process=True/False
            # If we don't need to assemble, we'll just skip this
            for node_type, nodes_indra_path in processor_to_assemble_paths.items():
                assembled_path = get_assembled_path(node_type)
                if force_assemble or (assemble and not assembled_path.exists()):
                    # Instantiate the assembler or add nodes to existing assembler
                    if node_type not in node_assemblers:
                        node_assemblers[node_type] = NodeAssembler()
                    click.secho(
                        f"Loading cached nodes from {nodes_indra_path}",
                        fg="green",
                    )
                    with open(nodes_indra_path, "rb") as fh:
                        nodes = pickle.load(fh)
                    node_assemblers[node_type].add_nodes(nodes)

    # Assemble nodes if we got any node assemblers above
    for node_type, assembler in node_assemblers.items():
        assembled_path = get_assembled_path(node_type)
        click.secho(f"Assembling {node_type}", fg="green")
        assembled_nodes = assembler.assemble_nodes()
        assembled_nodes = sorted(assembled_nodes, key=lambda x: (x.db_ns, x.db_id))
        Processor._dump_nodes_to_path_static(
            "assembled nodes", assembled_nodes, assembled_path
        )

    # The assembled paths are added to the list of nodes to import separately
    for node_type in to_assemble:
        assembled_path = get_assembled_path(node_type)
        if assembled_path.exists():
            nodes_paths_for_import.append(assembled_path)

    # Import the nodes
    if run_import:
        sudo_prefix = "" if not with_sudo else "sudo"
        command = dedent(
            f"""\
        {sudo_prefix} neo4j-admin database import full {'--force' if force_import else ''} \\
          --delimiter='TAB' \\
          --skip-duplicate-nodes=true \\
          --skip-bad-relationships=true \\
          --strict
        """
        ).rstrip()
        for node_path in nodes_paths_for_import:
            command += f"\\\n --nodes {node_path}"
        for edge_path in edge_paths:
            command += f"\\\n --relationships {edge_path}"
        # Specify the database name (if not the default "neo4j").
        # See https://neo4j.com/docs/operations-manual/5/tools/neo4j-admin/neo4j-admin-import/#_parameters
        command += "\\\n  indra"

        click.secho("Running shell command:")
        click.secho(command, fg="blue")
        os.system(command)  # noqa:S605


def get_pickle_paths() -> dict[str, list[Path]]:
    node_labels_to_processor_name_paths = defaultdict(list)
    for processor_cls in _iter_processors():
        if not processor_cls.importable:
            continue
        for label in processor_cls.node_types:
            (proc_nodes_path, nodes_indra_path, _, ) \
                = processor_cls._get_node_paths(label)
            node_labels_to_processor_name_paths[label].append(
                (processor_cls.name, proc_nodes_path, nodes_indra_path)
            )

    to_assemble = {}
    for label, processor_names_paths in node_labels_to_processor_name_paths.items():
        if len(processor_names_paths) > 1:
            to_assemble[label] = []
            for processor_name, proc_nodes_path, nodes_indra_path in processor_names_paths:
                to_assemble[label].append(nodes_indra_path)

    print(f"Node labels to assemble: {list(to_assemble)}")
    return to_assemble


def assemble_type(
    pickle_paths: list[Path],
    assembled_path: Path,
    force_assemble: bool = False,
):
    """Assemble nodes of a given type from the given pickle paths

    Parameters
    ----------
    pickle_paths :
        A list of the input paths to the pickled nodes
    assembled_path :
        The path to the output file where the assembled nodes will be saved
    force_assemble :
        If True, reassemble the nodes even if the output file already exists
    """
    if assembled_path.exists() and not force_assemble:
        print(f"Skipping assembly, {assembled_path} already exists")
        return
    na = NodeAssembler()
    for pickle_path in pickle_paths:
        print(f"Loading cached nodes from {pickle_path}")
        with open(pickle_path, "rb") as fh:
            nodes = pickle.load(fh)
        na.add_nodes(nodes)

    print(f"Assembling {len(na.nodes)} nodes")
    assembled_nodes = na.assemble_nodes()
    if na.conflicts:
        import gzip
        import csv
        # Replace nodes_{node_type}.tsv.gz with nodes_{node_type}_conflicts.tsv.gz
        conflict_path = assembled_path.with_name(
            assembled_path.stem + "_conflicts" + assembled_path.suffix
        )
        print(f"Got {len(na.conflicts)} conflicts, please inspect")
        with gzip.open(conflict_path, "wt", encoding="utf-8") as fh:
            writer = csv.writer(fh, delimiter="\t")
            writer.writerow(["key", "val1", "val2"])
            for conflict in na.conflicts:
                writer.writerow([conflict.key, conflict.val1, conflict.val2])

    print(f"Sorting {len(assembled_nodes)} assembled nodes")
    assembled_nodes = sorted(assembled_nodes, key=lambda x: (x.db_ns, x.db_id))
    Processor._dump_nodes_to_path_static(
        "assembled nodes", assembled_nodes, assembled_path
    )


def manual_assembly(force_assemble: bool = False):
    """Assemble nodes of a given type from the given pickle paths

    Parameters
    ----------
    force_assemble :
        If True, reassemble the nodes even if the output file already exists
    """
    to_assemble = get_pickle_paths()
    for node_type, paths in to_assemble.items():
        assembled_path = get_assembled_path(node_type)
        assemble_type(paths, assembled_path, force_assemble=force_assemble)


if __name__ == "__main__":
    main()
