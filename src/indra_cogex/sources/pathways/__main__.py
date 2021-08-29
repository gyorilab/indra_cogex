# -*- coding: utf-8 -*-

"""Run the pathways processor using ``python -m indra_cogex.sources.pathways``."""

import click
from more_click import verbose_option

from . import ReactomeProcessor, WikipathwaysProcessor


@click.command()
@verbose_option
@click.pass_context
def _main(ctx: click.Context):
    ctx.invoke(ReactomeProcessor.get_cli())
    ctx.invoke(WikipathwaysProcessor.get_cli())


if __name__ == "__main__":
    _main()
