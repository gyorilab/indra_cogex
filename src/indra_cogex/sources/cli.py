# -*- coding: utf-8 -*-

"""Run the sources CLI."""

import os
from textwrap import dedent

import click
from more_click import verbose_option

from . import processor_resolver


@click.command()
@verbose_option
def main():
    paths = []
    for processor_cls in processor_resolver:
        click.secho(f"Processing {processor_cls.name}", fg="green", bold=True)
        if not processor_cls.nodes_path.is_file() or not processor_cls.edges_path.is_file():
            processor = processor_cls()
            processor.dump()
        paths.append((processor_cls.nodes_path, processor_cls.edges_path))

    command = dedent(f'''\
    neo4j-admin import \\
      --database=indra \\
      --delimiter='TAB' \\
      --skip-duplicate-nodes=true \\
      --skip-bad-relationships=true
    ''').rstrip()
    for node_path, edge_path in paths:
        command += f'\\\n  --nodes {node_path} \\\n  --edges {edge_path}'

    click.secho('Running shell command:')
    click.secho(command, fg='blue')
    os.system(command)


if __name__ == "__main__":
    main()
