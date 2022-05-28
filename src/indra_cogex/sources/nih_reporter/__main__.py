"""Run the NIH Reporter processor using ``python -m indra_cogex.sources.nih_reporter``."""

from . import NihReporterProcessor

if __name__ == "__main__":
    NihReporterProcessor.cli()
