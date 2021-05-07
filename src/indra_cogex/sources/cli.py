# -*- coding: utf-8 -*-

"""Run the sources CLI."""

import click
import operator
from more_click import verbose_option

from . import processor_resolver


@click.command()
@verbose_option
def main():
    for processor_cls in sorted(processor_resolver.classes,
                                key=operator.attrgetter('__name__')):
        click.secho(f'Processing {processor_cls.name}', fg='green', bold=True)
        processor = processor_cls()
        _node_path, _edge_path = processor.dump()


if __name__ == '__main__':
    main()
