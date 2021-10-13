# -*- coding: utf-8 -*-

"""Run the pathways processor using ``python -m indra_cogex.sources.chembl``."""

from . import ChemblIndicationsProcessor

if __name__ == "__main__":
    ChemblIndicationsProcessor.cli()
