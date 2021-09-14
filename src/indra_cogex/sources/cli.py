# -*- coding: utf-8 -*-

"""Run the sources CLI."""

import os
import pickle
from textwrap import dedent

import click
import pystow
from more_click import verbose_option

from . import processor_resolver
from .processor import Processor
from ..assembly import NodeAssembler


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
@verbose_option
def main(load: bool, load_only: bool, force: bool, with_sudo: bool):
    """Generate and import Neo4j nodes and edges tables."""
    paths = []
    na = NodeAssembler()
    for processor_cls in processor_resolver:
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
                processor = processor_cls()
                click.secho("Processing...", fg="green")
                # FIXME: this is redundant, we get nodes twice
                nodes = list(processor.get_nodes())
                processor.dump()
            else:
                click.secho("Loading cached nodes...", fg="green")
                with open(processor_cls.nodes_indra_path, "rb") as fh:
                    nodes = pickle.load(fh)
            na.add_nodes(nodes)

        paths.append((processor_cls.nodes_path, processor_cls.edges_path))

    nodes_path = pystow.module("indra", "cogex", "assembled").join(name="nodes.tsv.gz")
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
