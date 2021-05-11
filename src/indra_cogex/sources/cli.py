# -*- coding: utf-8 -*-

"""Run the sources CLI."""

import os
from textwrap import dedent

import click
from more_click import verbose_option

from . import processor_resolver


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
@verbose_option
def main(load: bool, load_only: bool, force: bool):
    """Generate and import Neo4j nodes and edges tables."""
    paths = []
    for processor_cls in processor_resolver:
        click.secho(f"Checking {processor_cls.name}", fg="green", bold=True)
        if not load_only:
            if (
                force
                or not processor_cls.nodes_path.is_file()
                or not processor_cls.edges_path.is_file()
            ):
                click.secho("Processing...", fg="green")
                processor = processor_cls()
                processor.dump()
        paths.append((processor_cls.nodes_path, processor_cls.edges_path))

    if load or load_only:
        command = dedent(
            """\
        neo4j-admin import \\
          --database=indra \\
          --delimiter='TAB' \\
          --skip-duplicate-nodes=true \\
          --skip-bad-relationships=true
        """
        ).rstrip()
        for node_path, edge_path in paths:
            command += f"\\\n  --nodes {node_path} \\\n  --relationships {edge_path}"

        click.secho("Running shell command:")
        click.secho(command, fg="blue")
        os.system(command)  # noqa:S605


if __name__ == "__main__":
    main()
