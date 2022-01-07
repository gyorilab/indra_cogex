from indra_cogex.representation import node_query, norm_id, triple_query


def test_norm_id():
    assert norm_id("UP", "P12345") == "uniprot:P12345"
    assert norm_id("CHEBI", "CHEBI:12345") == "chebi:12345"


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
