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
    assert len(nodes) == 9

    assert nodes[0].db_id == "5"
    assert nodes[1].db_id == "186"
    assert nodes[2].db_id == "3084"
    assert nodes[3].db_id == "7646"
    assert nodes[4].db_id == "29037"

    assert nodes[5].db_id == "127399_SOFT_TISSUE"
    assert nodes[6].db_id == "DMS53_LUNG"
    assert nodes[7].db_id == "NCIH1694_LUNG"
    assert nodes[8].db_id == "SW1116_LARGE_INTESTINE"


def test_get_relations():
    cna_path = os.path.join(os.path.dirname(__file__), "test_data_cna.txt")
    mutations_path = os.path.join(
        os.path.dirname(__file__), "test_data_mutations_extended.txt"
    )
    cp = CbioportalProcessor(cna_path=cna_path, mutations_path=mutations_path)
    relations = []
    relations.extend(cp.get_relations())
    assert len(relations) == 4
    assert relations[0].rel_type == "copy_number_altered_in"
    assert relations[0].source_id == "7646"
    assert relations[0].target_id == "DMS53_LUNG"
    assert relations[1].rel_type == "copy_number_altered_in"
    assert relations[1].source_id == "7646"
    assert relations[1].target_id == "SW1116_LARGE_INTESTINE"
    assert relations[2].rel_type == "mutated_in"
    assert relations[2].source_id == "3084"
    assert relations[2].target_id == "127399_SOFT_TISSUE"
    assert relations[3].rel_type == "mutated_in"
    assert relations[3].source_id == "29037"
    assert relations[3].target_id == "127399_SOFT_TISSUE"
