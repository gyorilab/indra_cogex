# -*- coding: utf-8 -*-

"""Run the pathways processor using ``python -m indra_cogex.sources.pathways``."""

from . import ReactomeProcessor, WikipathwaysProcessor

if __name__ == "__main__":
    ReactomeProcessor.cli()
    WikipathwaysProcessor.cli()
