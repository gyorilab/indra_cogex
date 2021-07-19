from . import CcleCnaProcessor, CcleMutationsProcessor, CcleDrugResponseProcessor

if __name__ == "__main__":
    CcleCnaProcessor.cli()
    CcleMutationsProcessor.cli()
    CcleDrugResponseProcessor.cli()
