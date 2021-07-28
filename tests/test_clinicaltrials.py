from indra_cogex.sources.clinicaltrials import ClinicaltrialsProcessor
import os


def test_get_nodes():
    path = os.path.join(os.path.dirname(__file__), "test_clinical_trials.csv")
    cp = ClinicaltrialsProcessor(path)
    nodes = list(cp.get_nodes())
    assert len(nodes) != 0
    assert len(nodes) >= len(cp.has_trial_cond_ns) + len(cp.tested_in_int_ns)

    assert len(cp.has_trial_cond_ns) != 0
    assert len(cp.has_trial_cond_ns) == len(cp.has_trial_cond_id)
    assert len(cp.has_trial_cond_id) == len(cp.has_trial_nct)

    assert len(cp.tested_in_int_ns) != 0
    assert len(cp.tested_in_int_ns) == len(cp.tested_in_int_id)
    assert len(cp.tested_in_int_id) == len(cp.tested_in_nct)

    for node in nodes:
        assert node.db_ns != ""
        assert node.db_id != ""


def test_get_relations():
    path = os.path.join(os.path.dirname(__file__), "test_clinical_trials.csv")
    cp = ClinicaltrialsProcessor(path)
    nodes = list(cp.get_nodes())
    relations = list(cp.get_relations())
    assert len(relations) != 0
    for relation in relations:
        assert relation.source_ns != ""
        assert relation.source_id != ""
        assert relation.target_ns == 'CLINICALTRIALS'
        assert relation.target_id != ""
        assert relation.rel_type == "has_trial" or relation.rel_type == "tested_in"
