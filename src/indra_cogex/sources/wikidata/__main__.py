"""
Run the Wikidata processors using ``python -m indra_cogex.sources.wikidata``.
"""

from . import JournalPublisherProcessor

if __name__ == "__main__":
    JournalPublisherProcessor.cli()
