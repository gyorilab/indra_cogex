from indra_cogex.sources.cbioportal import CbioportalProcessor
import os


def test_get_nodes():
    cna_path = os.path.join(os.path.dirname(__file__), "test_data_cna.txt")
    mutations_path = os.path.join(
        os.path.dirname(__file__), "test_data_mutations_extended.txt"
    )
    cp = CbioportalProcessor(cna_path=cna_path, mutations_path=mutations_path)
    nodes = []
    nodes.extend(cp.get_nodes())
    assert len(nodes) == 8
    assert nodes[0].db_id == "5"
    assert nodes[1].db_id == "7646"
    assert nodes[2].db_id == "186"
    assert nodes[3].db_id == "DMS53_LUNG"
    assert nodes[4].db_id == "SW1116_LARGE_INTESTINE"
    assert nodes[5].db_id == "NCIH1694_LUNG"
    assert nodes[6].db_id == "3084"
    assert nodes[7].db_id == "127399_SOFT_TISSUE"


def test_get_relations():
    path = os.path.join(os.path.dirname(__file__), "test_data_cna.txt")
    cp = CbioportalProcessor(cna_path=path)
    relations = []
    relations.extend(cp.get_relations())
    assert len(relations) == 2
    assert relations[0].rel_type == "copy_number_altered_in"
    assert relations[0].source_id == "7646"
    assert relations[0].target_id == "DMS53_LUNG"
    assert relations[1].rel_type == "copy_number_altered_in"
    assert relations[1].source_id == "7646"
    assert relations[1].target_id == "SW1116_LARGE_INTESTINE"
