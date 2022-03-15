"""Run the enzyme code processor using ``python -m indra_cogex.sources.ec``."""

from . import HGNCEnzymeProcessor

if __name__ == "__main__":
    HGNCEnzymeProcessor.cli()
