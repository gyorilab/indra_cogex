from indra.config import get_config
from indra.statements import Agent

from indra_cogex.neo4j_client import Neo4jClient

nc = Neo4jClient(
    get_config("INDRA_NEO4J_URL"),
    auth=(get_config("INDRA_NEO4J_USER"), get_config("INDRA_NEO4J_PASSWORD")),
)


def test_get_targets():
    targets = nc.get_targets("HGNC:6871", "isa")
    assert len(targets) == 1
    assert targets[0]["name"] == "ERK"

    agent = nc.node_to_agent(targets[0])
    assert isinstance(agent, Agent)


def test_get_target_agents():
    targets = nc.get_target_agents("HGNC:6871", "isa")
    assert len(targets) == 1
    assert isinstance(targets[0], Agent)


def test_get_common_targets():
    targets1 = nc.get_targets("HGNC:7597", "expressed_in")
    targets2 = nc.get_targets("HGNC:7599", "expressed_in")
    targets = nc.get_common_targets(["HGNC:7597", "HGNC:7599"], "expressed_in")
    assert len(targets) > 200
    assert {t["id"] for t in targets1} & {t["id"] for t in targets2} == {
        t["id"] for t in targets
    }


def test_get_sources():
    sources = nc.get_sources("FPLX:ERK", "isa")
    assert len(sources) == 2
    assert {s["id"] for s in sources} == {"MAPK1", "MAPK3"}, sources
