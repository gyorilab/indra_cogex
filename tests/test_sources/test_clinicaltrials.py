from indra_cogex.sources.clinicaltrials import ClinicaltrialsProcessor
from indra_cogex.sources.processor_util import data_validator


def test_get_nodes():
    cp = ClinicaltrialsProcessor(max_pages=1)
    nodes = list(cp.get_nodes())
    assert nodes
    assert any(
        node.db_ns.lower() == "clinicaltrials" and
        node.db_id.startswith("NCT") for node in nodes
    )

    for node in nodes:
        assert set(node.labels) & {"BioEntity", "ClinicalTrial"}
        for prop_key, value in node.data.items():
            data_type = prop_key.split(":")[1] if ":" in prop_key else "string"
            data_validator(data_type, value)

def test_get_relations():
    cp = ClinicaltrialsProcessor(max_pages=1)
    relations = list(cp.get_relations())
    assert relations
    for relation in relations:
        assert relation.source_ns
        assert relation.source_id
        assert relation.target_ns.lower() == "clinicaltrials"
        assert relation.target_id
        assert relation.target_id.startswith("NCT")
        assert relation.rel_type in {"has_trial", "tested_in"}
        for prop_key, value in relation.data.items():
            data_type = prop_key.split(":")[1] if ":" in prop_key else "string"
            data_validator(data_type, value)
