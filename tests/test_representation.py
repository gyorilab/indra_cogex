from indra_cogex.representation import (
    node_query,
    norm_id,
    triple_query,
    triple_parameter_query,
    dump_norm_id
)


def test_norm_id():
    assert norm_id("UP", "P12345") == "uniprot:P12345"
    assert norm_id("CHEBI", "CHEBI:12345") == "chebi:12345"
    assert norm_id("CHEBI", "12345") == "chebi:12345"
    assert norm_id("chebi", "12345") == "chebi:12345"
    try:
        # Should error with AttributeError with current implementation
        # If implementation changes it will be caught here
        assert norm_id("indra_evidence", "175613") == "indra_evidence:175613"
    except AttributeError:
        pass


def test_dump_norm_id():
    assert dump_norm_id("indra_evidence", "175613") == "indra_evidence:175613"


def test_node_query():
    """Test generating node query strings."""
    assert node_query() == ""
    assert node_query(node_name="s") == "s"
    assert node_query(node_type="Type") == ":Type"
    assert node_query(node_id="1234") == "{id: '1234'}"
    assert node_query(node_name="s", node_type="Type") == "s:Type"
    assert node_query(node_name="s", node_id="1234") == "s {id: '1234'}"
    assert node_query(node_type="Type", node_id="1234") == ":Type {id: '1234'}"
    assert (
        node_query(node_name="s", node_type="Type", node_id="1234")
        == "s:Type {id: '1234'}"
    )


def test_triple_query():
    """Test generating the match part of triples queries."""
    # example like in Neo4jClient.get_relations
    assert (
        triple_query(
            source_name="s",
            source_id=norm_id("UP", "P12345"),
            relation_type="relation",
            target_name="t",
            target_id=norm_id("CHEBI", "CHEBI:12345"),
        )
        == "(s {id: 'uniprot:P12345'})-[:relation]->(t {id: 'chebi:12345'})"
    )

    # example like in Neo4jClient.get_common_sources and Neo4jClient.get_predecessors
    assert (
        triple_query(
            source_name="s",
            relation_type="relation",
            target_id=norm_id("CHEBI", "CHEBI:12345"),
        )
        == "(s)-[:relation]->({id: 'chebi:12345'})"
    )

    # example like in Neo4jClient.get_common_targets and Neo4jClient.get_successors
    assert (
        triple_query(
            source_id=norm_id("UP", "P12345"), relation_type="relation", target_name="t"
        )
        == "({id: 'uniprot:P12345'})-[:relation]->(t)"
    )


def test_triple_param_query():
    assert triple_parameter_query(
        source_name="s",
        source_type="BioEntity",
        relation_type="indra_rel",
        relation_direction="left",
        target_type="BioEntity",
        target_prop_name="id",
        target_prop_param="mesh_term",
    ) == "(s:BioEntity)<-[:indra_rel]-(:BioEntity {id: $mesh_term})"

    assert triple_parameter_query(
        source_name="s",
        source_type="BioEntity",
        source_prop_name="id",
        source_prop_param="mesh_term",
        relation_name="r",
        relation_type="indra_rel",
        relation_direction="right",
        target_name="t",
        target_type="BioEntity",
        target_prop_name="id",
        target_prop_param="mesh_term",
    ) == \
           "(s:BioEntity {id: $mesh_term})-[r:indra_rel]->(t:BioEntity {id: $mesh_term})"
