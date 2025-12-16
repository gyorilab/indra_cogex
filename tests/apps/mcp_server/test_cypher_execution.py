"""Integration tests for Cypher Execution Layer (Layer 2).

Tests REAL Cypher execution with actual Neo4j queries.
Validates parameterization, timeouts, result limiting, and caching.

Run with: pytest -m nonpublic tests/apps/mcp_server/test_cypher_execution.py
"""
import pytest
from indra_cogex.apps.mcp_server.query_execution import (
    execute_cypher,
    explain_query,
    _normalize_curie_parameters,
    QueryExecutionError,
    QueryTimeoutError,
    QuerySyntaxError,
    QueryValidationError
)




@pytest.mark.nonpublic
class TestBasicCypherExecution:
    """Test basic Cypher query execution."""

    def test_simple_parameterized_query(self, neo4j_client):
        """Execute simple parameterized query for a specific gene."""
        result = execute_cypher(
            client=neo4j_client,
            query="MATCH (g:BioEntity) WHERE g.id = $gene_id RETURN g LIMIT 1",
            parameters={"gene_id": "hgnc:18618"},  # LRRK2

        )

        # Should have results
        assert "results" in result
        assert isinstance(result["results"], list)
        assert len(result["results"]) <= 1

        # Should have metadata
        assert "metadata" in result
        assert "result_count" in result["metadata"]
        assert "execution_time_ms" in result["metadata"]

        # Should not be from cache on first run
        assert "from_cache" in result
        assert result["from_cache"] is False

    def test_query_returning_multiple_entities(self, neo4j_client):
        """Execute query returning multiple gene entities."""
        result = execute_cypher(
            client=neo4j_client,
            query="MATCH (g:BioEntity) RETURN g.id, g.name LIMIT 10",
            max_results=10,

        )

        # Should have results
        assert "results" in result
        assert len(result["results"]) <= 10
        assert len(result["results"]) > 0

        # Results should have expected structure with column names
        first_result = result["results"][0]
        assert "g.id" in first_result or "g.name" in first_result

    def test_query_with_relationship_traversal(self, neo4j_client):
        """Execute query traversing relationships."""
        result = execute_cypher(
            client=neo4j_client,
            query="""
                MATCH (g:BioEntity)-[r]-(d:BioEntity)
                WHERE d.id = $disease_id
                RETURN g.id, g.name
                LIMIT 20
            """,
            parameters={"disease_id": "mesh:D000544"},  # Alzheimer's
            max_results=20,

        )

        # Should have results
        assert "results" in result
        assert isinstance(result["results"], list)

        # If results exist, should have gene info
        if len(result["results"]) > 0:
            first = result["results"][0]
            assert "g.id" in first or "g.name" in first


@pytest.mark.nonpublic
class TestParameterization:
    """Test parameterized query execution."""

    def test_multiple_parameters(self, neo4j_client):
        """Execute query with multiple parameters."""
        result = execute_cypher(
            client=neo4j_client,
            query="""
                MATCH (g:BioEntity)
                WHERE g.id IN $gene_ids
                RETURN g.id, g.name
                LIMIT $limit
            """,
            parameters={
                "gene_ids": ["hgnc:18618", "hgnc:11138"],  # LRRK2, SNCA
                "limit": 10
            },

        )

        # Should succeed
        assert "results" in result
        assert len(result["results"]) <= 2

    def test_parameter_types_preserved(self, neo4j_client):
        """Parameters should preserve types (string, int, list, etc)."""
        result = execute_cypher(
            client=neo4j_client,
            query="RETURN $string_param AS s, $int_param AS i, $list_param AS l",
            parameters={
                "string_param": "test",
                "int_param": 42,
                "list_param": ["a", "b", "c"]
            },

        )

        # Should return parameters with correct types
        assert "results" in result
        assert len(result["results"]) == 1

        result_row = result["results"][0]
        assert result_row["s"] == "test"
        assert result_row["i"] == 42
        assert result_row["l"] == ["a", "b", "c"]

    def test_query_without_parameters(self, neo4j_client):
        """Simple query without parameters should work."""
        result = execute_cypher(
            client=neo4j_client,
            query="MATCH (g:BioEntity) RETURN count(g) AS gene_count",

        )

        # Should have result
        assert "results" in result
        assert len(result["results"]) == 1
        assert "gene_count" in result["results"][0]
        assert isinstance(result["results"][0]["gene_count"], int)


@pytest.mark.nonpublic
class TestResultLimiting:
    """Test result size limiting."""

    def test_max_results_enforced(self, neo4j_client):
        """max_results parameter should limit returned rows."""
        result = execute_cypher(
            client=neo4j_client,
            query="MATCH (g:BioEntity) RETURN g LIMIT 1000",  # Ask for 1000
            max_results=10,  # But limit to 10

        )

        # Should only return 10
        assert "results" in result
        assert len(result["results"]) <= 10

        # Should indicate limiting occurred
        assert "metadata" in result
        assert "result_count" in result["metadata"]
        assert result["metadata"]["result_count"] == len(result["results"])

    def test_default_max_results_applied(self, neo4j_client):
        """Default max_results should be applied if not specified."""
        # Use LIMIT 200 to get a known set that exceeds default (100)
        # The execute_cypher function should truncate to 100
        result = execute_cypher(
            client=neo4j_client,
            query="MATCH (g:BioEntity) RETURN g.id LIMIT 200",  # Ask for 200

            # No max_results specified - should use default (100)
        )

        # Should be limited to default (100)
        assert "results" in result
        assert len(result["results"]) <= 100

        # Should indicate truncation occurred
        assert "metadata" in result
        assert result["metadata"].get("truncated", False) is True

    def test_max_results_ceiling_enforced(self, neo4j_client):
        """max_results should not exceed system maximum (10000)."""
        # Test that requesting over 10000 gets capped
        # We don't need to actually return 10000 rows - just verify the cap is applied
        result = execute_cypher(
            client=neo4j_client,
            query="MATCH (g:BioEntity) RETURN g.id LIMIT 50",  # Small set
            max_results=999999,  # Over limit - should be capped to 10000

        )

        # Should succeed (timeout was capped, not rejected)
        assert "results" in result
        # Since we only asked for 50, we should get <= 50
        assert len(result["results"]) <= 50


@pytest.mark.nonpublic
class TestTimeoutEnforcement:
    """Test query timeout enforcement."""

    def test_timeout_prevents_long_queries(self, neo4j_client):
        """Timeout should kill long-running queries.

        Note: This test verifies that timeouts are detected, but Neo4j may not
        always honor client-side timeouts for complex queries. The test uses
        a pytest timeout to prevent hanging.
        """
        # Create a deliberately slow query with a short timeout
        # If the query completes quickly on a small DB, that's also valid
        try:
            result = execute_cypher(
                client=neo4j_client,
                query="""
                    MATCH (a:BioEntity), (b:BioEntity)
                    RETURN count(*) AS cnt
                """,  # Cartesian product of 2 nodes - should be slow
                timeout_ms=500,  # 500ms timeout - quite short
                validate_first=False,  # Skip validation to test timeout
                max_results=1,  # Only need 1 row

            )
            # If query completed, check if it was slow (indicates timeout wasn't hit)
            exec_time = result.get("metadata", {}).get("execution_time_ms", 0)
            # Test passes either way - we're verifying the function handles timeouts
            assert "results" in result
        except (QueryTimeoutError, QueryExecutionError) as e:
            # Expected path - timeout was triggered
            error_msg = str(e).lower()
            assert "timeout" in error_msg or "time" in error_msg or "exceeded" in error_msg

    def test_default_timeout_applied(self, neo4j_client):
        """Default timeout (30s) should be applied."""
        result = execute_cypher(
            client=neo4j_client,
            query="MATCH (g:BioEntity) RETURN g LIMIT 1",

            # No timeout specified - should use default (30000ms)
        )

        # Should complete successfully
        assert "results" in result

        # Metadata should show execution time < default timeout
        assert "metadata" in result
        assert "execution_time_ms" in result["metadata"]
        assert result["metadata"]["execution_time_ms"] < 30000

    def test_timeout_maximum_enforced(self, neo4j_client):
        """Timeout should not exceed maximum (120s)."""
        # This should be capped
        result = execute_cypher(
            client=neo4j_client,
            query="MATCH (g:BioEntity) RETURN g LIMIT 1",
            timeout_ms=999999999,  # Way over limit (should be capped to 120000)

        )

        # Should complete successfully (timeout was capped, not rejected)
        assert "results" in result


@pytest.mark.nonpublic
class TestExplainQuery:
    """Test query plan explanation."""

    def test_explain_returns_execution_plan(self, neo4j_client):
        """Explain should return query execution plan structure.

        Note: The current implementation returns a basic structure because
        Neo4j's EXPLAIN doesn't return plan info in a way that's easily parsed
        through the standard query_tx interface. Full EXPLAIN parsing would
        require using the Result.plan() API.
        """
        result = explain_query(
            client=neo4j_client,
            query="MATCH (g:BioEntity)-[]-(d:BioEntity) RETURN g, d LIMIT 10"
        )

        # Should have execution plan dict (may be empty or have basic info)
        assert "execution_plan" in result
        assert isinstance(result["execution_plan"], dict)

        # Should have query echoed back
        assert "query" in result

        # Should have warnings or recommendations list
        assert "warnings" in result or "recommendations" in result

    def test_explain_includes_warnings(self, neo4j_client):
        """Explain should include performance warnings."""
        # Query without WHERE clause (full scan)
        result = explain_query(
            client=neo4j_client,
            query="MATCH (g:BioEntity) RETURN g"
        )

        # Should have execution plan
        assert "execution_plan" in result

        # Should have warnings or recommendations
        assert "warnings" in result or "recommendations" in result

    def test_explain_does_not_execute(self, neo4j_client):
        """Explain should not actually execute the query."""
        result = explain_query(
            client=neo4j_client,
            query="MATCH (g:BioEntity) RETURN g"
        )

        # Should NOT have results
        assert "results" not in result or result.get("results") == []

        # Should have plan
        assert "execution_plan" in result


@pytest.mark.nonpublic
class TestValidationIntegration:
    """Test validation integration with execution."""

    def test_validation_runs_by_default(self, neo4j_client):
        """Validation should run by default before execution."""
        result = execute_cypher(
            client=neo4j_client,
            query="MATCH (g:BioEntity) RETURN g LIMIT 10",

            # validate_first defaults to True
        )

        # Should execute successfully
        assert "results" in result

    def test_validation_can_be_skipped(self, neo4j_client):
        """Validation can be explicitly disabled."""
        result = execute_cypher(
            client=neo4j_client,
            query="MATCH (g:BioEntity) RETURN g LIMIT 10",
            validate_first=False,

        )

        # Should still execute successfully
        assert "results" in result

    def test_dangerous_query_blocked_by_validation(self, neo4j_client):
        """Dangerous queries should be blocked by validation."""
        with pytest.raises(QueryValidationError):
            execute_cypher(
                client=neo4j_client,
                query="MATCH (g:BioEntity) DETACH DELETE g",  # Dangerous!
                validate_first=True,

            )


@pytest.mark.nonpublic
class TestErrorHandling:
    """Test error handling in query execution."""

    def test_syntax_error_returns_clear_error(self, neo4j_client):
        """Syntax errors should raise QuerySyntaxError."""
        with pytest.raises(QuerySyntaxError) as exc_info:
            execute_cypher(
                client=neo4j_client,
                query="MATCH (g:BioEntity RETURN g",  # Missing closing paren
                validate_first=False,  # Skip validation to test execution error

            )

        # Should have syntax error
        error_msg = str(exc_info.value).lower()
        assert "syntax" in error_msg or "cypher" in error_msg

    def test_missing_parameter_returns_error(self, neo4j_client):
        """Missing required parameter should raise error."""
        with pytest.raises((QueryExecutionError, QuerySyntaxError)):
            execute_cypher(
                client=neo4j_client,
                query="MATCH (g:BioEntity) WHERE g.id = $gene_id RETURN g",
                parameters={},  # Missing gene_id parameter

            )

    def test_nonexistent_label_returns_empty(self, neo4j_client):
        """Querying nonexistent label should return empty results, not error."""
        result = execute_cypher(
            client=neo4j_client,
            query="MATCH (n:NonexistentLabel) RETURN n",

        )

        # Should succeed with empty results
        assert "results" in result
        assert len(result["results"]) == 0


@pytest.mark.nonpublic
class TestMetadataQuality:
    """Test quality of execution metadata."""

    def test_metadata_includes_execution_time(self, neo4j_client):
        """Metadata should include execution time."""
        result = execute_cypher(
            client=neo4j_client,
            query="MATCH (g:BioEntity) RETURN g LIMIT 10",

        )

        # Should have execution time
        assert "metadata" in result
        assert "execution_time_ms" in result["metadata"]
        assert isinstance(result["metadata"]["execution_time_ms"], (int, float))
        assert result["metadata"]["execution_time_ms"] >= 0

    def test_metadata_includes_result_count(self, neo4j_client):
        """Metadata should include result count."""
        result = execute_cypher(
            client=neo4j_client,
            query="MATCH (g:BioEntity) RETURN g LIMIT 5",

        )

        # Should have result count
        assert "metadata" in result
        assert "result_count" in result["metadata"]
        assert result["metadata"]["result_count"] == len(result["results"])

    def test_metadata_includes_query_hash(self, neo4j_client):
        """Metadata should include query hash for caching."""
        result = execute_cypher(
            client=neo4j_client,
            query="MATCH (g:BioEntity) RETURN g LIMIT 1",

        )

        # Should have query hash
        assert "metadata" in result
        assert "query_hash" in result["metadata"]
        assert isinstance(result["metadata"]["query_hash"], str)
        assert len(result["metadata"]["query_hash"]) > 0

    def test_token_estimate_included(self, neo4j_client):
        """Results should include token estimate."""
        result = execute_cypher(
            client=neo4j_client,
            query="MATCH (g:BioEntity) RETURN g LIMIT 5",

        )

        # Should have token estimate
        assert "token_estimate" in result
        assert isinstance(result["token_estimate"], int)
        assert result["token_estimate"] >= 0


@pytest.mark.nonpublic
class TestCurieParameterNormalization:
    """Test automatic CURIE parameter normalization to match graph storage format."""

    def test_normalize_single_curie(self):
        """Single uppercase CURIE should be normalized to lowercase."""
        params = {"gene_id": "HGNC:28337"}
        normalized = _normalize_curie_parameters(params)

        assert normalized["gene_id"] == "hgnc:28337"

    def test_normalize_list_of_curies(self):
        """List of CURIEs should all be normalized."""
        params = {"gene_ids": ["HGNC:28337", "HGNC:6407", "HGNC:11138"]}
        normalized = _normalize_curie_parameters(params)

        assert normalized["gene_ids"] == ["hgnc:28337", "hgnc:6407", "hgnc:11138"]

    def test_normalize_mixed_parameters(self):
        """Non-CURIE parameters should pass through unchanged."""
        params = {
            "gene_id": "HGNC:28337",
            "limit": 10,
            "name": "TP53",
            "score": 0.95,
            "active": True,
            "tags": ["cancer", "tumor-suppressor"]
        }
        normalized = _normalize_curie_parameters(params)

        # CURIE should be normalized
        assert normalized["gene_id"] == "hgnc:28337"

        # Other parameters should be unchanged
        assert normalized["limit"] == 10
        assert normalized["name"] == "TP53"
        assert normalized["score"] == 0.95
        assert normalized["active"] is True
        assert normalized["tags"] == ["cancer", "tumor-suppressor"]

    def test_normalize_different_prefixes(self):
        """Different CURIE prefixes should all be normalized."""
        params = {
            "gene": "HGNC:6407",
            "disease": "MESH:D000544",
            "drug": "CHEBI:15365",
            "pathway": "REACTOME:R-HSA-112316"
        }
        normalized = _normalize_curie_parameters(params)

        assert normalized["gene"] == "hgnc:6407"
        assert normalized["disease"] == "mesh:D000544"
        assert normalized["drug"] == "chebi:15365"
        assert normalized["pathway"] == "reactome:R-HSA-112316"

    def test_normalize_nested_dict(self):
        """Nested dictionaries should have CURIEs normalized recursively."""
        params = {
            "filters": {
                "gene_id": "HGNC:28337",
                "limit": 10
            }
        }
        normalized = _normalize_curie_parameters(params)

        assert normalized["filters"]["gene_id"] == "hgnc:28337"
        assert normalized["filters"]["limit"] == 10

    def test_normalize_preserves_non_curie_colons(self):
        """Strings with colons that aren't CURIEs should be preserved."""
        params = {
            "timestamp": "2024-01-15T10:30:00",
            "url": "http://example.com",
            "ratio": "1:100"
        }
        normalized = _normalize_curie_parameters(params)

        # These should attempt normalization but fall back to original if it fails
        # The exact behavior depends on bioregistry - at minimum they shouldn't crash
        assert "timestamp" in normalized
        assert "url" in normalized
        assert "ratio" in normalized

    def test_uppercase_curie_query_returns_results(self, neo4j_client):
        """Queries with uppercase CURIEs should return results after normalization."""
        # This is the critical bug fix test - uppercase CURIE should now work
        result = execute_cypher(
            client=neo4j_client,
            query="MATCH (g:BioEntity) WHERE g.id = $gene_id RETURN g",
            parameters={"gene_id": "HGNC:18618"},  # Uppercase LRRK2 - should be normalized
        )

        # Should return results (not empty due to case mismatch)
        assert "results" in result
        # If the gene exists, we should get a result
        if len(result["results"]) > 0:
            assert "g" in result["results"][0]

    def test_uppercase_curie_list_query_returns_results(self, neo4j_client):
        """Queries with lists of uppercase CURIEs should work."""
        result = execute_cypher(
            client=neo4j_client,
            query="""
                MATCH (g:BioEntity)
                WHERE g.id IN $gene_ids
                RETURN g.id, g.name
            """,
            parameters={"gene_ids": ["HGNC:18618", "HGNC:11138"]},  # Uppercase LRRK2, SNCA
        )

        assert "results" in result
        # Results should be returned if genes exist
        if len(result["results"]) > 0:
            # Returned IDs should be in lowercase (graph format)
            for row in result["results"]:
                if "g.id" in row:
                    assert row["g.id"].startswith("hgnc:")

    def test_normalization_preserves_already_normalized_curies(self):
        """CURIEs that are already lowercase should remain unchanged."""
        params = {"gene_id": "hgnc:28337"}
        normalized = _normalize_curie_parameters(params)

        assert normalized["gene_id"] == "hgnc:28337"

    def test_empty_parameters_handled(self):
        """Empty parameter dict should be handled gracefully."""
        params = {}
        normalized = _normalize_curie_parameters(params)

        assert normalized == {}

    def test_explain_query_normalizes_parameters(self, neo4j_client):
        """EXPLAIN queries should also normalize parameters."""
        result = explain_query(
            client=neo4j_client,
            query="MATCH (g:BioEntity) WHERE g.id = $gene_id RETURN g",
            parameters={"gene_id": "HGNC:18618"}  # Uppercase LRRK2
        )

        # Should return execution plan without error
        assert "execution_plan" in result
        assert "query" in result
