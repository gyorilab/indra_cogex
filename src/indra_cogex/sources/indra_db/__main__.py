# -*- coding: utf-8 -*-

"""Run the INDRA database processor using ``python -m indra_cogex.sources.indra_db``."""

from . import DbProcessor, EvidenceProcessor

if __name__ == "__main__":
    DbProcessor.cli()
    EvidenceProcessor.cli()
