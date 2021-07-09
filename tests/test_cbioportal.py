from indra_cogex.sources.cbioportal import CbioportalProcessor
import os


def test_get_nodes():
    path = os.path.join(os.path.dirname(__file__), "test_data_cna.txt")
    cp = CbioportalProcessor(path)
    nodes = []
    nodes.extend(cp.get_nodes())
    assert len(nodes) == 3
