import pytest

from indra.config import get_config
from indra.statements import Agent

from indra_cogex.client.neo4j_client import Neo4jClient, process_identifier


def _get_client():
    nc = Neo4jClient(
        get_config("INDRA_NEO4J_URL"),
        auth=(get_config("INDRA_NEO4J_USER"), get_config("INDRA_NEO4J_PASSWORD")),
    )
    return nc


@pytest.mark.nonpublic
def test_get_targets():
    nc = _get_client()
    targets = nc.get_targets(("HGNC", "6871"), relation="isa")
    assert len(targets) == 1
    assert targets[0].data["name"] == "ERK"

    agent = nc.node_to_agent(targets[0])
    assert isinstance(agent, Agent)


@pytest.mark.nonpublic
def test_get_target_agents():
    nc = _get_client()
    targets = nc.get_target_agents(("HGNC", "6871"), "isa")
    assert len(targets) == 1
    assert isinstance(targets[0], Agent)


@pytest.mark.nonpublic
def test_get_common_targets():
    nc = _get_client()
    targets1 = nc.get_targets(("HGNC", "7597"), "expressed_in")
    targets2 = nc.get_targets(("HGNC", "7599"), "expressed_in")
    targets = nc.get_common_targets(
        [("HGNC", "7597"), ("HGNC", "7599")], "expressed_in"
    )
    assert len(targets) > 200
    assert {t.db_id for t in targets1} & {t.db_id for t in targets2} == {
        t.db_id for t in targets
    }


@pytest.mark.nonpublic
def test_get_sources():
    nc = _get_client()
    sources = nc.get_sources(("FPLX", "ERK"), "isa")
    assert len(sources) == 2
    assert {s.data["name"] for s in sources} == {"MAPK1", "MAPK3"}, sources


def test_process_identifier():
    assert process_identifier("hgnc:6871") == ("HGNC", "6871")
    assert process_identifier("chebi:1234") == ("CHEBI", "CHEBI:1234")
    assert process_identifier("uploc:SL-0086") == ("UPLOC", "SL-0086")


@pytest.mark.nonpublic
def test_get_source_relations():
    nc = _get_client()
    relations = nc.get_source_relations(
        target=("HGNC", "9875"),
        relation="indra_rel",
        source_type='BioEntity',
        target_type='BioEntity',
    )

    assert relations[0].target_name == "RASGRF1"
