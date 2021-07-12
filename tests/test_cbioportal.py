from indra_cogex.sources.cbioportal import CbioportalProcessor
import os


def test_get_nodes():
    path = os.path.join(os.path.dirname(__file__), "test_data_cna.txt")
    cp = CbioportalProcessor(path)
    nodes = []
    nodes.extend(cp.get_nodes())
    assert len(nodes) == 6
    assert nodes[0].db_id == "A1BG"
    assert nodes[1].db_id == "NAT2"
    assert nodes[2].db_id == "ADA"
    assert nodes[3].db_id == "DMS53_LUNG"
    assert nodes[4].db_id == "SW1116_LARGE_INTESTINE"
    assert nodes[5].db_id == "NCIH1694_LUNG"


def test_get_relations():
    path = os.path.join(os.path.dirname(__file__), "test_data_cna.txt")
    cp = CbioportalProcessor(path)
    relations = []
    relations.extend(cp.get_relations())
    assert len(relations) == 2
    assert relations[0].rel_type == "copy_number_altered_in"
    assert relations[0].source_id == "NAT2"
    assert relations[0].target_id == "DMS53_LUNG"
    assert relations[1].rel_type == "copy_number_altered_in"
    assert relations[1].source_id == "NAT2"
    assert relations[1].target_id == "SW1116_LARGE_INTESTINE"
