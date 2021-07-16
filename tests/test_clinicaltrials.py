from indra_cogex.sources.clinicaltrials import ClinicaltrialsProcessor
import os


def test_get_nodes():
    path = os.path.join(os.path.dirname(__file__), "test_search_results.tsv")
    cp = ClinicaltrialsProcessor(path)
    nodes = list(cp.get_nodes())
    assert len(nodes) is not 0
