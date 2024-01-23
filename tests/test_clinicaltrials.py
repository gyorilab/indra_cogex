from indra_cogex.sources.clinicaltrials import ClinicaltrialsProcessor, \
    get_correct_mesh_id
import os


def test_get_nodes():
    path = os.path.join(os.path.dirname(__file__), "test_clinical_trials.csv")
    cp = ClinicaltrialsProcessor(path)
    nodes = list(cp.get_nodes())
    assert nodes
    assert len(nodes) >= len(cp.has_trial_cond_ns) + len(cp.tested_in_int_ns)

    assert cp.has_trial_cond_ns
    assert len(cp.has_trial_cond_ns) == len(cp.has_trial_cond_id)
    assert len(cp.has_trial_cond_id) == len(cp.has_trial_nct)

    assert cp.tested_in_int_ns
    assert len(cp.tested_in_int_ns) == len(cp.tested_in_int_id)
    assert len(cp.tested_in_int_id) == len(cp.tested_in_nct)

    for node in nodes:
        assert node.db_ns != ""
        assert node.db_id != ""


def test_get_relations():
    path = os.path.join(os.path.dirname(__file__), "test_clinical_trials.csv")
    cp = ClinicaltrialsProcessor(path)
    nodes = list(cp.get_nodes())
    assert nodes
    relations = list(cp.get_relations())
    assert relations
    for relation in relations:
        assert relation.source_ns
        assert relation.source_id
        assert relation.target_ns == "CLINICALTRIALS"
        assert relation.target_id
        assert relation.rel_type == "has_trial" or relation.rel_type == "tested_in"


def test_get_correct_mesh_id():
    assert get_correct_mesh_id('D013274')[0] == 'D013274'
    assert get_correct_mesh_id('D000013274')[0] == 'D013274'
    assert get_correct_mesh_id('C000603933', 'Osimertinib')[0] == 'C000596361'