# -*- coding: utf-8 -*-

"""Run the sources CLI."""

import json
import os
import pickle
from collections import Counter
from operator import itemgetter
from pathlib import Path
from textwrap import dedent
from typing import Iterable, Optional, TextIO, Type

import click
import pystow
from more_click import verbose_option
from tabulate import tabulate

from . import processor_resolver
from .processor import Processor
from ..assembly import NodeAssembler

MODULE = pystow.module("indra", "cogex", "assembled")
DEFAULT_NODES_PATH = MODULE.join(name="nodes.tsv.gz")
NODES_SUMMARY_PATH = MODULE.join(name="nodes_summary.tsv")
EDGES_SUMMARY_PATH = MODULE.join(name="edges_summary.tsv")


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
    "--nodes-path",
    default=DEFAULT_NODES_PATH,
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
    nodes_path,
    allow_missing: bool,
):
    """Generate and import Neo4j nodes and edges tables."""
    nodes_path = Path(nodes_path)
    config = {} if config is None else json.load(config)
    paths = []
    na = NodeAssembler()
    global_edge_counter = {}
    for processor_cls in _iter_resolvers():
        if not processor_cls.importable:
            continue
        click.secho(processor_cls.name, bold=True)
        if not load_only:
            if (
                force
                or not processor_cls.nodes_path.is_file()
                or not processor_cls.nodes_indra_path.is_file()
                or not processor_cls.edges_path.is_file()
                or not processor_cls.edges_summary_path.is_file()
            ):
                try:
                    processor = processor_cls(**config.get(processor_cls.name, {}))
                except FileNotFoundError as e:
                    if not allow_missing:
                        raise
                    click.secho(f"  Failed: {e}", fg="red")
                    continue
                click.secho("  Processing...", fg="green")
                _, nodes, _, edge_counter = processor.dump()
            else:
                click.secho(
                    f"  Loading cached nodes from {processor_cls.nodes_indra_path}",
                    fg="green",
                )
                with open(processor_cls.nodes_indra_path, "rb") as fh:
                    nodes = pickle.load(fh)

                edge_counter = Counter()
                with processor_cls.edges_summary_path.open() as file:
                    _ = next(file)
                    for line in file:
                        name, count = line.strip().split("\t")
                        edge_counter[name] = int(count)

            for rel, count in edge_counter.items():
                global_edge_counter[
                    processor_cls.name,
                    rel,
                    processor_cls.descriptions.get(rel, ""),
                ] = count
            na.add_nodes(nodes)

        paths.append((processor_cls.nodes_path, processor_cls.edges_path))

    edge_summary_rows = sorted(
        global_edge_counter.items(),
        key=itemgetter(1),
    )
    edge_summary = tabulate(
        ((name, rel, desc, count) for (name, rel, desc), count in edge_summary_rows),
        headers=["Processor", "Type", "Description", "Count"],
        tablefmt="github",
    )
    click.echo(edge_summary)
    EDGES_SUMMARY_PATH.write_text(edge_summary + "\n")

    if not load_only:
        if force or not nodes_path.is_file():
            # Now create and dump the assembled nodes
            assembled_nodes = na.assemble_nodes()
            assembled_nodes = sorted(assembled_nodes, key=lambda x: (x.db_ns, x.db_id))
            Processor._dump_nodes_to_path(assembled_nodes, nodes_path)

    if load or load_only:
        sudo_prefix = "" if not with_sudo else "sudo"
        command = dedent(
            f"""\
        {sudo_prefix} neo4j-admin import \\
          --database=indra \\
          --delimiter='TAB' \\
          --skip-duplicate-nodes=true \\
          --skip-bad-relationships=true \\
          --nodes {nodes_path}
        """
        ).rstrip()
        for _, edge_path in paths:
            command += f"\\\n  --relationships {edge_path}"

        click.secho("Running shell command:")
        click.secho(command, fg="blue")
        os.system(command)  # noqa:S605


if __name__ == "__main__":
    main()
