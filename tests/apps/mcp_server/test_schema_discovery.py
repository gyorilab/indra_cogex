"""Integration tests for Schema Discovery Layer (Layer 1).

Tests REAL schema discovery with actual Neo4j graph metadata.
Validates progressive disclosure, caching, and token estimation.

Run with: pytest -m nonpublic tests/apps/mcp_server/test_schema_discovery.py
"""
import json
import pytest
from indra_cogex.apps.mcp_server.schema_discovery import get_graph_schema

# Fixtures neo4j_client and flask_app_with_client are provided by conftest.py




@pytest.mark.nonpublic
class TestSchemaDiscoveryBasics:
    """Test basic schema discovery at different detail levels."""

    def test_summary_level_returns_basic_stats(self, neo4j_client):
        """Summary level should return high-level graph statistics."""
        result = get_graph_schema(neo4j_client, detail_level="summary")

        # Should have core statistics
        assert "total_entities" in result
        assert "total_relationships" in result
        assert "entity_types" in result
        assert "relationship_types" in result

        # Should be lists of names only at summary level
        assert isinstance(result["entity_types"], list)
        assert isinstance(result["relationship_types"], list)

        # Should have common entity types (check for BioEntity-related types)
        entity_types = result["entity_types"]
        assert len(entity_types) > 0

        # Should have token estimate
        assert "token_estimate" in result
        assert isinstance(result["token_estimate"], int)
        assert result["token_estimate"] > 0

        # Should have metadata
        assert "detail_level" in result
        assert result["detail_level"] == "summary"
        assert "execution_time_ms" in result

    def test_entity_types_level_includes_counts_and_properties(self, neo4j_client):
        """Entity types level should include counts and property details."""
        result = get_graph_schema(neo4j_client, detail_level="entity_types")

        # Should have detailed entity information
        assert "entity_details" in result
        assert isinstance(result["entity_details"], list)
        assert len(result["entity_details"]) > 0

        # Check first entity has required fields
        first_entity = result["entity_details"][0]
        assert "label" in first_entity
        assert "count" in first_entity
        assert "properties" in first_entity
        # count may be None if not computed for this detail level
        assert first_entity["count"] is None or isinstance(first_entity["count"], int)
        assert isinstance(first_entity["properties"], list)

        # Should include sample IDs
        assert "sample_ids" in first_entity
        assert isinstance(first_entity["sample_ids"], list)

        # Should still have summary data
        assert "entity_types" in result
        assert "relationship_types" in result

    def test_relationship_types_level_includes_edge_details(self, neo4j_client):
        """Relationship types level should include edge type details."""
        result = get_graph_schema(neo4j_client, detail_level="relationship_types")

        # Should have relationship details
        assert "relationship_details" in result
        assert isinstance(result["relationship_details"], list)
        assert len(result["relationship_details"]) > 0

        # Check first relationship has required fields
        first_rel = result["relationship_details"][0]
        assert "type" in first_rel
        assert "count" in first_rel
        # count may be None if not computed for this detail level
        assert first_rel["count"] is None or isinstance(first_rel["count"], int)

        # Should have source/target entity types
        assert "source_types" in first_rel
        assert "target_types" in first_rel
        assert isinstance(first_rel["source_types"], list)
        assert isinstance(first_rel["target_types"], list)

        # Should have properties
        assert "properties" in first_rel
        assert isinstance(first_rel["properties"], list)

    def test_patterns_level_includes_relationship_patterns(self, neo4j_client):
        """Patterns level should include relationship patterns."""
        result = get_graph_schema(neo4j_client, detail_level="patterns")

        # Should have relationship patterns
        assert "relationship_patterns" in result
        assert isinstance(result["relationship_patterns"], list)

        if len(result["relationship_patterns"]) > 0:
            first_pattern = result["relationship_patterns"][0]

            # Should have pattern structure
            assert "pattern" in first_pattern
            assert "count" in first_pattern
            assert "description" in first_pattern
            assert "example_query" in first_pattern

            # Pattern should be in format (Source)-[:REL_TYPE]->(Target)
            assert "(" in first_pattern["pattern"]
            assert ")" in first_pattern["pattern"]
            assert "-[" in first_pattern["pattern"]
            assert "]->" in first_pattern["pattern"]

            # Example query should be valid Cypher
            assert "MATCH" in first_pattern["example_query"]
            assert "RETURN" in first_pattern["example_query"]

    def test_full_level_returns_complete_schema(self, neo4j_client):
        """Full level should return comprehensive schema information."""
        result = get_graph_schema(neo4j_client, detail_level="full")

        # Should have all detail levels combined
        assert "entity_details" in result
        assert "relationship_details" in result
        assert "relationship_patterns" in result
        assert "total_entities" in result
        assert "total_relationships" in result

        # Should have both summary lists
        assert "entity_types" in result
        assert "relationship_types" in result

        # Token estimate should be higher for full schema
        assert "token_estimate" in result
        assert result["token_estimate"] > 1000  # Full schema should be substantial


@pytest.mark.nonpublic
class TestSchemaFiltering:
    """Test schema filtering by entity and relationship types."""

    def test_filter_by_entity_type(self, neo4j_client):
        """Should filter schema to specific entity type."""
        # First get summary to find a valid entity type
        summary = get_graph_schema(neo4j_client, detail_level="summary")
        if not summary["entity_types"]:
            pytest.skip("No entity types found in graph")

        entity_type = summary["entity_types"][0]

        # Now filter to that entity type
        result = get_graph_schema(
            neo4j_client,
            detail_level="entity_types",
            entity_type=entity_type,

        )

        # Should only include specified entity type
        entity_details = result["entity_details"]
        assert len(entity_details) == 1
        assert entity_details[0]["label"] == entity_type

        # Should have properties for that type
        assert "properties" in entity_details[0]
        assert "count" in entity_details[0]

    def test_filter_by_relationship_type(self, neo4j_client):
        """Should filter schema to specific relationship type."""
        # First get summary to find a valid relationship type
        summary = get_graph_schema(neo4j_client, detail_level="summary")
        if not summary["relationship_types"]:
            pytest.skip("No relationship types found in graph")

        relationship_type = summary["relationship_types"][0]

        # Now filter to that relationship type
        result = get_graph_schema(
            neo4j_client,
            detail_level="relationship_types",
            relationship_type=relationship_type,

        )

        # Should only include specified relationship type
        rel_details = result["relationship_details"]
        assert len(rel_details) == 1
        assert rel_details[0]["type"] == relationship_type

    def test_filter_both_entity_and_relationship(self, neo4j_client):
        """Should filter by both entity and relationship types."""
        # Get summary to find valid types
        summary = get_graph_schema(neo4j_client, detail_level="summary")
        if not summary["entity_types"] or not summary["relationship_types"]:
            pytest.skip("Insufficient types in graph")

        entity_type = summary["entity_types"][0]
        relationship_type = summary["relationship_types"][0]

        result = get_graph_schema(
            neo4j_client,
            detail_level="full",
            entity_type=entity_type,
            relationship_type=relationship_type,

        )

        # Should have filtered results
        assert "entity_details" in result
        assert len(result["entity_details"]) == 1
        assert result["entity_details"][0]["label"] == entity_type

        assert "relationship_details" in result
        assert len(result["relationship_details"]) == 1
        assert result["relationship_details"][0]["type"] == relationship_type


@pytest.mark.nonpublic
class TestSchemaCaching:
    """Test schema caching behavior.

    Note: Schema queries are no longer cached (cache simplified to grounding only).
    These tests verify schema queries execute consistently.
    """

    def test_schema_queries_execute_consistently(self, neo4j_client):
        """Repeated schema queries should return consistent results."""
        # First query
        result1 = get_graph_schema(neo4j_client, detail_level="summary")
        assert "entity_types" in result1

        # Second query
        result2 = get_graph_schema(neo4j_client, detail_level="summary")
        assert "entity_types" in result2

        # Results should be consistent
        assert result1["entity_types"] == result2["entity_types"]
        assert result1["relationship_types"] == result2["relationship_types"]

    def test_different_detail_levels_return_different_detail(self, neo4j_client):
        """Different detail levels should return appropriately different content."""
        result_summary = get_graph_schema(neo4j_client, detail_level="summary")
        result_full = get_graph_schema(neo4j_client, detail_level="full")

        # Full should have more detail than summary
        assert result_full["token_estimate"] > result_summary["token_estimate"]

        # Full should have entity_details that summary doesn't
        assert "entity_details" in result_full
        assert "relationship_details" in result_full


@pytest.mark.nonpublic
class TestTokenEstimation:
    """Test token estimation accuracy."""

    def test_token_estimates_increase_with_detail(self, neo4j_client):
        """Token estimates should increase with detail level."""
        # Get all detail levels
        levels = ["summary", "entity_types", "relationship_types", "full"]
        estimates = {}

        for level in levels:
            result = get_graph_schema(neo4j_client, detail_level=level)
            estimates[level] = result["token_estimate"]

        # Full should be highest
        assert estimates["full"] >= estimates["relationship_types"]
        assert estimates["full"] >= estimates["entity_types"]
        assert estimates["full"] >= estimates["summary"]

    def test_token_estimate_reflects_result_size(self, neo4j_client):
        """Token estimate should correlate with actual result size."""
        result = get_graph_schema(neo4j_client, detail_level="full")

        # Token estimate should be reasonable for full schema
        assert "token_estimate" in result

        # Rough check: token count should be non-trivial
        assert result["token_estimate"] > 100

        # Result size should correlate (roughly 4 chars per token)
        result_chars = len(json.dumps(result))
        estimated_tokens = result["token_estimate"]

        # Very rough check - within order of magnitude
        assert result_chars / 10 < estimated_tokens < result_chars


@pytest.mark.nonpublic
class TestSchemaValidation:
    """Test schema validation and error handling."""

    def test_invalid_detail_level_raises_error(self, neo4j_client):
        """Invalid detail level should raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            get_graph_schema(neo4j_client, detail_level="invalid_level")

        assert "Invalid detail_level" in str(exc_info.value)
        assert "summary" in str(exc_info.value)

    def test_nonexistent_entity_type_handles_gracefully(self, neo4j_client):
        """Querying nonexistent entity type should return empty results."""
        result = get_graph_schema(
            neo4j_client,
            detail_level="entity_types",
            entity_type="NonexistentEntityType123456789",

        )

        # Should not error, just return empty entity_details
        assert "entity_details" in result
        # Either empty or has one entry with count 0
        if len(result["entity_details"]) > 0:
            assert result["entity_details"][0]["count"] == 0

    def test_nonexistent_relationship_type_handles_gracefully(self, neo4j_client):
        """Querying nonexistent relationship type should return empty or zero-count results."""
        result = get_graph_schema(
            neo4j_client,
            detail_level="relationship_types",
            relationship_type="NONEXISTENT_RELATIONSHIP_123456789",

        )

        # Should not error, just return empty or zero-count relationship_details
        assert "relationship_details" in result
        # Either empty list or entry with count 0
        if len(result["relationship_details"]) > 0:
            assert result["relationship_details"][0]["count"] == 0

    def test_schema_returns_valid_structure_always(self, neo4j_client):
        """Schema query should always return valid structure."""
        result = get_graph_schema(neo4j_client, detail_level="summary")

        # Should return valid structure
        assert "total_entities" in result
        assert "total_relationships" in result
        assert "entity_types" in result
        assert "relationship_types" in result
        assert "detail_level" in result
        assert "execution_time_ms" in result
        assert "token_estimate" in result

        # Types should be correct
        assert isinstance(result["total_entities"], (int, type(None)))
        assert isinstance(result["total_relationships"], (int, type(None)))
        assert isinstance(result["entity_types"], list)
        assert isinstance(result["relationship_types"], list)
        assert isinstance(result["detail_level"], str)
        assert isinstance(result["execution_time_ms"], int)
        assert isinstance(result["token_estimate"], int)
