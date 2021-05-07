# -*- coding: utf-8 -*-

"""Run the sources CLI."""

import click
from more_click import verbose_option

from . import processor_resolver


@click.command()
@verbose_option
def main():
    for processor_cls in processor_resolver:
        click.secho(f"Processing {processor_cls.name}", fg="green", bold=True)
        processor = processor_cls()
        _node_path, _edge_path = processor.dump()


if __name__ == "__main__":
    main()
