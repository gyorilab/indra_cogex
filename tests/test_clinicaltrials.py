from src.indra_cogex.sources.clinicaltrials import ClinicaltrialsProcessor
import os


def test_get_nodes():
    path = os.path.join(os.path.dirname(__file__), "test_search_results_2.tsv")
    cp = ClinicaltrialsProcessor(path)
    nodes = list(cp.get_nodes())
    assert len(nodes) is not 0


def test_get_relations():
    path = os.path.join(os.path.dirname(__file__), "test_search_results_2.tsv")
    cp = ClinicaltrialsProcessor(path)
    relations = list(cp.get_relations())
    assert len(relations) is not 0
