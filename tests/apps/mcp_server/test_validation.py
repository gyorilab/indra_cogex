"""Tests for Cypher query validation (Layer 3).

Tests the minimal validation layer:
1. Dangerous keyword blocking (defense-in-depth)
2. Parameter completeness verification

Neo4j driver enforces read-only via execute_read().
Neo4j provides superior syntax error messages.

Run with: pytest tests/apps/mcp_server/test_validation.py -v
"""
import pytest
from indra_cogex.apps.mcp_server.validation import validate_cypher


class TestDangerousKeywordBlocking:
    """Test that dangerous operations are blocked."""

    def test_delete_blocked(self):
        """DELETE operations should be blocked."""
        result = validate_cypher("MATCH (g:Gene) DELETE g")
        assert result["valid"] is False
        assert result["safe_to_execute"] is False
        assert any("DELETE" in issue["message"] for issue in result["issues"])

    def test_detach_delete_blocked(self):
        """DETACH DELETE should be blocked."""
        result = validate_cypher("MATCH (g:Gene) DETACH DELETE g")
        assert result["valid"] is False
        assert any("DETACH" in issue["message"] or "DELETE" in issue["message"]
                   for issue in result["issues"])

    def test_create_blocked(self):
        """CREATE operations should be blocked."""
        result = validate_cypher("CREATE (g:Gene {id: 'test'})")
        assert result["valid"] is False
        assert any("CREATE" in issue["message"] for issue in result["issues"])

    def test_merge_blocked(self):
        """MERGE operations should be blocked."""
        result = validate_cypher("MERGE (g:Gene {id: 'test'}) RETURN g")
        assert result["valid"] is False
        assert any("MERGE" in issue["message"] for issue in result["issues"])

    def test_set_blocked(self):
        """SET operations should be blocked."""
        result = validate_cypher("MATCH (g:Gene) SET g.name = 'test'")
        assert result["valid"] is False
        assert any("SET" in issue["message"] for issue in result["issues"])

    def test_remove_blocked(self):
        """REMOVE operations should be blocked."""
        result = validate_cypher("MATCH (g:Gene) REMOVE g.obsolete")
        assert result["valid"] is False
        assert any("REMOVE" in issue["message"] for issue in result["issues"])

    def test_drop_blocked(self):
        """DROP operations should be blocked."""
        result = validate_cypher("DROP INDEX gene_id")
        assert result["valid"] is False
        assert any("DROP" in issue["message"] for issue in result["issues"])


class TestReadOperationsAllowed:
    """Test that read operations pass validation."""

    def test_simple_match_return(self):
        """Basic MATCH/RETURN should pass."""
        result = validate_cypher("MATCH (g:Gene) RETURN g LIMIT 10")
        assert result["valid"] is True
        assert result["safe_to_execute"] is True
        assert len(result["issues"]) == 0

    def test_match_with_where(self):
        """MATCH with WHERE clause should pass."""
        result = validate_cypher(
            "MATCH (g:Gene) WHERE g.id = $id RETURN g",
            parameters={"id": "hgnc:18618"}
        )
        assert result["valid"] is True
        assert result["safe_to_execute"] is True

    def test_relationship_traversal(self):
        """Relationship traversal should pass."""
        result = validate_cypher(
            "MATCH (g:Gene)-[:ASSOCIATES_WITH]->(d:Disease) RETURN g, d LIMIT 100"
        )
        assert result["valid"] is True

    def test_with_clause(self):
        """WITH clause should pass."""
        result = validate_cypher(
            "MATCH (g:Gene) WITH g ORDER BY g.name RETURN g LIMIT 5"
        )
        assert result["valid"] is True

    def test_unwind(self):
        """UNWIND should pass."""
        result = validate_cypher("UNWIND [1, 2, 3] AS x RETURN x")
        assert result["valid"] is True

    def test_aggregation(self):
        """Aggregation functions should pass."""
        result = validate_cypher("MATCH (g:Gene) RETURN count(g)")
        assert result["valid"] is True

    def test_optional_match(self):
        """OPTIONAL MATCH should pass."""
        result = validate_cypher(
            "MATCH (g:Gene) OPTIONAL MATCH (g)-[:HAS]->(p) RETURN g, p LIMIT 10"
        )
        assert result["valid"] is True


class TestParameterValidation:
    """Test parameter completeness checking."""

    def test_all_parameters_provided(self):
        """Query with all required parameters should pass."""
        result = validate_cypher(
            "MATCH (g:Gene) WHERE g.id = $id RETURN g",
            parameters={"id": "hgnc:18618"}
        )
        assert result["valid"] is True
        assert not any("missing_parameter" in issue["type"] for issue in result["issues"])

    def test_missing_parameter_detected(self):
        """Query with missing parameters should fail."""
        result = validate_cypher(
            "MATCH (g:Gene) WHERE g.id = $gene_id RETURN g",
            parameters={}
        )
        assert result["valid"] is False
        assert any(
            issue["type"] == "missing_parameter" and "gene_id" in issue["message"]
            for issue in result["issues"]
        )

    def test_multiple_missing_parameters(self):
        """Multiple missing parameters should all be reported."""
        result = validate_cypher(
            "MATCH (g:Gene) WHERE g.id = $id AND g.name = $name RETURN g",
            parameters={}
        )
        assert result["valid"] is False
        missing = [i for i in result["issues"] if i["type"] == "missing_parameter"]
        assert len(missing) == 2

    def test_extra_parameters_ok(self):
        """Extra unused parameters should not cause errors."""
        result = validate_cypher(
            "MATCH (g:Gene) WHERE g.id = $id RETURN g",
            parameters={"id": "hgnc:18618", "unused": "value"}
        )
        assert result["valid"] is True

    def test_no_parameters_needed(self):
        """Query without parameters should pass with empty dict."""
        result = validate_cypher(
            "MATCH (g:Gene) RETURN g LIMIT 10",
            parameters={}
        )
        assert result["valid"] is True


class TestEmptyQueryHandling:
    """Test edge cases for empty/whitespace queries."""

    def test_empty_string_rejected(self):
        """Empty string should be rejected."""
        result = validate_cypher("")
        assert result["valid"] is False
        assert any(issue["type"] == "empty_query" for issue in result["issues"])

    def test_whitespace_only_rejected(self):
        """Whitespace-only string should be rejected."""
        result = validate_cypher("   \n\t  ")
        assert result["valid"] is False

    def test_none_handled(self):
        """None query should be handled gracefully."""
        result = validate_cypher(None)
        assert result["valid"] is False


class TestResultStructure:
    """Test that validation returns proper structure."""

    def test_valid_query_structure(self):
        """Valid query should return proper structure."""
        result = validate_cypher("MATCH (g:Gene) RETURN g LIMIT 10")

        assert "valid" in result
        assert isinstance(result["valid"], bool)

        assert "issues" in result
        assert isinstance(result["issues"], list)

        assert "safe_to_execute" in result
        assert isinstance(result["safe_to_execute"], bool)

    def test_invalid_query_structure(self):
        """Invalid query should return proper structure with issues."""
        result = validate_cypher("MATCH (g:Gene) DELETE g")

        assert result["valid"] is False
        assert result["safe_to_execute"] is False
        assert len(result["issues"]) > 0

        # Check issue structure
        issue = result["issues"][0]
        assert "severity" in issue
        assert "type" in issue
        assert "message" in issue
        assert issue["severity"] == "error"


class TestCaseInsensitivity:
    """Test that keyword detection is case-insensitive."""

    def test_lowercase_delete_blocked(self):
        """lowercase delete should be blocked."""
        result = validate_cypher("match (g:Gene) delete g")
        assert result["valid"] is False

    def test_mixed_case_create_blocked(self):
        """Mixed case Create should be blocked."""
        result = validate_cypher("Create (g:Gene {id: 'test'})")
        assert result["valid"] is False

    def test_uppercase_match_allowed(self):
        """UPPERCASE MATCH should be allowed."""
        result = validate_cypher("MATCH (G:GENE) RETURN G LIMIT 10")
        assert result["valid"] is True
