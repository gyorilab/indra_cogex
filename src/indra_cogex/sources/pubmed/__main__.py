"""Run the PubMed processor using ``python -m indra_cogex.sources.pubmed``."""

from . import PubmedProcessor


if __name__ == "__main__":
    PubmedProcessor.cli()
