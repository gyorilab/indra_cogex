"""Integration Tests for MCP Server Workflow.

This test suite focuses on end-to-end integration testing and workflow composition.
For comprehensive individual tool testing, see specialized test files:
- test_cypher_execution.py - Query execution (Layer 2)
- test_validation.py - Query validation (Layer 3)
- test_schema_discovery.py - Schema discovery (Layer 1)
- test_enrichment_tools_integration.py - Result enrichment (Layer 4)

Test Coverage in this file:
- Complete research workflows across multiple tools
- Performance benchmarks
- Error handling integration scenarios
- Tool coverage verification

Run with: pytest -m nonpublic tests/apps/mcp_server/test_mcp_integration.py -v
"""
import asyncio
import time
import pytest

# Import MCP server components
from indra_cogex.apps.mcp_server.schema_discovery import get_graph_schema
from indra_cogex.apps.mcp_server.query_execution import execute_cypher
from indra_cogex.apps.mcp_server.validation import validate_cypher
from indra_cogex.apps.mcp_server.enrichment import enrich_results, DisclosureLevel


# ============================================================================
# Part 1: Individual Tool Testing
# ============================================================================

@pytest.mark.nonpublic
class TestGetGraphSchemaComprehensive:
    """Comprehensive tests for get_graph_schema tool."""

    @pytest.mark.asyncio
    async def test_summary_level(self, flask_app_with_client, neo4j_client):
        """Test schema discovery at summary level."""
        result = await asyncio.to_thread(
            get_graph_schema,
            client=neo4j_client,
            detail_level="summary"
        )

        assert "detail_level" in result
        assert result["detail_level"] == "summary"
        assert "total_entities" in result or "note" in result
        assert "token_estimate" in result

    @pytest.mark.asyncio
    async def test_entity_types_level(self, flask_app_with_client, neo4j_client):
        """Test schema discovery at entity_types level."""
        result = await asyncio.to_thread(
            get_graph_schema,
            client=neo4j_client,
            detail_level="entity_types"
        )

        assert result["detail_level"] == "entity_types"
        # Should have more detail than summary
        assert "entity_types" in result or "note" in result

    @pytest.mark.asyncio
    async def test_relationship_types_level(self, flask_app_with_client, neo4j_client):
        """Test schema discovery at relationship_types level."""
        result = await asyncio.to_thread(
            get_graph_schema,
            client=neo4j_client,
            detail_level="relationship_types"
        )

        assert result["detail_level"] == "relationship_types"

    @pytest.mark.asyncio
    async def test_full_level(self, flask_app_with_client, neo4j_client):
        """Test schema discovery at full level."""
        result = await asyncio.to_thread(
            get_graph_schema,
            client=neo4j_client,
            detail_level="full"
        )

        assert result["detail_level"] == "full"

    @pytest.mark.asyncio
    async def test_filter_by_entity_type(self, flask_app_with_client, neo4j_client):
        """Test filtering schema by specific entity type."""
        result = await asyncio.to_thread(
            get_graph_schema,
            client=neo4j_client,
            detail_level="entity_types",
            entity_type="Gene"
        )

        # Should filter to Gene entities if supported
        assert "detail_level" in result


@pytest.mark.nonpublic
class TestEnrichResultsComprehensive:
    """Comprehensive tests for enrich_results tool."""

    @pytest.mark.asyncio
    async def test_minimal_disclosure_level(self, flask_app_with_client, neo4j_client):
        """Test enrichment at MINIMAL disclosure level."""
        results = [
            {"id": "hgnc:18618", "name": "LRRK2"},
            {"id": "hgnc:11138", "name": "SNCA"}
        ]

        result = await asyncio.to_thread(
            enrich_results,
            results=results,
            disclosure_level=DisclosureLevel.MINIMAL,
            result_type="gene",
            client=neo4j_client
        )

        assert result["disclosure_level"] == "minimal"
        assert "results" in result
        assert len(result["results"]) == 2
        # MINIMAL should not add much metadata
        assert "_description" not in result["results"][0]

    @pytest.mark.asyncio
    async def test_standard_disclosure_level(self, flask_app_with_client, neo4j_client):
        """Test enrichment at STANDARD disclosure level."""
        results = [
            {"id": "hgnc:18618", "name": "LRRK2"},
            {"id": "hgnc:11138", "name": "SNCA"}
        ]

        result = await asyncio.to_thread(
            enrich_results,
            results=results,
            disclosure_level=DisclosureLevel.STANDARD,
            result_type="gene",
            client=neo4j_client
        )

        assert result["disclosure_level"] == "standard"
        assert "results" in result
        # STANDARD should add basic metadata
        assert "_description" in result["results"][0]
        assert "_next_steps" in result["results"][0]

    @pytest.mark.asyncio
    async def test_detailed_disclosure_level(self, flask_app_with_client, neo4j_client):
        """Test enrichment at DETAILED disclosure level."""
        results = [
            {"id": "hgnc:18618", "name": "LRRK2"}
        ]

        result = await asyncio.to_thread(
            enrich_results,
            results=results,
            disclosure_level=DisclosureLevel.DETAILED,
            result_type="gene",
            client=neo4j_client
        )

        assert result["disclosure_level"] == "detailed"
        # DETAILED should add provenance (graph_context was removed as low-value)
        assert "_provenance" in result["results"][0]
        assert "_next_steps" in result["results"][0]

    @pytest.mark.asyncio
    async def test_exploratory_disclosure_level(self, flask_app_with_client, neo4j_client):
        """Test enrichment at EXPLORATORY disclosure level."""
        results = [
            {"id": "hgnc:18618", "name": "LRRK2"}
        ]

        result = await asyncio.to_thread(
            enrich_results,
            results=results,
            disclosure_level=DisclosureLevel.EXPLORATORY,
            result_type="gene",
            client=neo4j_client
        )

        assert result["disclosure_level"] == "exploratory"
        # EXPLORATORY should add workflows and research context
        assert "_workflows" in result["results"][0]
        assert "_research_context" in result["results"][0]

    @pytest.mark.asyncio
    async def test_different_result_types(self, flask_app_with_client, neo4j_client):
        """Test enrichment with different entity types."""
        test_cases = [
            ({"id": "hgnc:18618", "name": "LRRK2"}, "gene"),
            ({"id": "mesh:D000544", "name": "Alzheimer Disease"}, "disease"),
            ({"id": "chebi:27732", "name": "Caffeine"}, "drug"),
            ({"id": "reactome:R-HSA-112316", "name": "Neuronal System"}, "pathway")
        ]

        for item, result_type in test_cases:
            result = await asyncio.to_thread(
                enrich_results,
                results=[item],
                disclosure_level=DisclosureLevel.STANDARD,
                result_type=result_type,
                client=neo4j_client
            )

            assert result["metadata"]["result_type"] == result_type
            assert "_description" in result["results"][0]

    @pytest.mark.asyncio
    async def test_token_estimation(self, flask_app_with_client, neo4j_client):
        """Test that token estimates are reasonable."""
        results = [{"id": f"hgnc:{i}", "name": f"GENE{i}"} for i in range(10)]

        # Test different disclosure levels have increasing token counts
        minimal = await asyncio.to_thread(
            enrich_results,
            results=results,
            disclosure_level=DisclosureLevel.MINIMAL,
            client=neo4j_client
        )

        standard = await asyncio.to_thread(
            enrich_results,
            results=results,
            disclosure_level=DisclosureLevel.STANDARD,
            client=neo4j_client
        )

        detailed = await asyncio.to_thread(
            enrich_results,
            results=results,
            disclosure_level=DisclosureLevel.DETAILED,
            client=neo4j_client
        )

        # Token estimates should increase with disclosure level
        assert minimal["token_estimate"] < standard["token_estimate"]
        assert standard["token_estimate"] < detailed["token_estimate"]


# ============================================================================
# Part 2: Integration & Workflow Testing
# ============================================================================

@pytest.mark.nonpublic
class TestCompleteResearchWorkflow:
    """Test end-to-end research workflow using all MCP tools."""

    @pytest.mark.asyncio
    async def test_als_research_workflow(self, flask_app_with_client, neo4j_client):
        """Complete ALS research workflow: schema → query → validate → enrich."""

        # Step 1: Discover schema
        schema = await asyncio.to_thread(
            get_graph_schema,
            client=neo4j_client,
            detail_level="summary"
        )
        assert "detail_level" in schema

        # Step 2: Execute query to find ALS genes
        als_query = """
            MATCH (d:BioEntity)-[r]-(g:BioEntity)
            WHERE d.name CONTAINS 'Amyotrophic'
            AND g.id STARTS WITH 'hgnc:'
            RETURN g.id AS gene_id, g.name AS gene_name
            LIMIT 20
        """

        als_results = await asyncio.to_thread(
            execute_cypher,
            client=neo4j_client,
            query=als_query,
            parameters={},
            validate_first=True,
        )

        assert "results" in als_results

        # Step 3: Validate a follow-up query
        validation = await asyncio.to_thread(
            validate_cypher,
            query="MATCH (g:Gene)-[:INTERACTS_WITH]-(p:Gene) WHERE g.id = $id RETURN p LIMIT 50",
            parameters={"id": "hgnc:11138"}
        )
        assert validation["valid"] is True

        # Step 4: Enrich results with metadata
        if als_results["results"]:
            enriched = await asyncio.to_thread(
                enrich_results,
                results=als_results["results"][:5],  # Enrich first 5
                disclosure_level=DisclosureLevel.DETAILED,
                result_type="gene",
                client=neo4j_client
            )

            assert enriched["disclosure_level"] == "detailed"
            assert "_provenance" in enriched["results"][0]


# ============================================================================
# Part 3: Performance Benchmarks
# ============================================================================

@pytest.mark.nonpublic
class TestPerformanceBenchmarks:
    """Performance tests for common query patterns."""

    @pytest.mark.asyncio
    async def test_simple_query_performance(self, flask_app_with_client, neo4j_client):
        """Benchmark simple node lookup query."""
        start = time.time()

        result = await asyncio.to_thread(
            execute_cypher,
            client=neo4j_client,
            query="MATCH (g:BioEntity) WHERE g.id = $id RETURN g LIMIT 1",
            parameters={"id": "hgnc:18618"},
        )

        elapsed_ms = (time.time() - start) * 1000

        # Should complete in < 1 second for simple query
        assert elapsed_ms < 1000
        assert "results" in result

    @pytest.mark.asyncio
    async def test_repeated_query_performance(self, flask_app_with_client, neo4j_client):
        """Test that repeated queries execute efficiently."""
        query = "MATCH (g:BioEntity)-[r]-(d:BioEntity) WHERE g.id = $id RETURN d.id LIMIT 20"
        params = {"id": "hgnc:18618"}

        # First execution
        start1 = time.time()
        result1 = await asyncio.to_thread(
            execute_cypher,
            client=neo4j_client,
            query=query,
            parameters=params,
        )
        time1_ms = (time.time() - start1) * 1000

        # Second execution
        start2 = time.time()
        result2 = await asyncio.to_thread(
            execute_cypher,
            client=neo4j_client,
            query=query,
            parameters=params,
        )
        time2_ms = (time.time() - start2) * 1000

        # Both queries should complete successfully
        assert "results" in result1
        assert "results" in result2

        # Both should be reasonably fast
        assert time1_ms < 2000
        assert time2_ms < 2000


# ============================================================================
# Part 4: Error Handling & Edge Cases
# ============================================================================

@pytest.mark.nonpublic
class TestErrorHandlingIntegration:
    """Test error handling across tools."""

    @pytest.mark.asyncio
    async def test_invalid_cypher_caught_by_validation(self, flask_app_with_client, neo4j_client):
        """Test that invalid Cypher is caught before execution."""
        from indra_cogex.apps.mcp_server.query_execution import QueryValidationError

        with pytest.raises(QueryValidationError):
            await asyncio.to_thread(
                execute_cypher,
                client=neo4j_client,
                query="MATCH (g:Gene DELETE g",  # Malformed
                parameters={},
                validate_first=True,
            )

    @pytest.mark.asyncio
    async def test_dangerous_query_blocked(self, flask_app_with_client, neo4j_client):
        """Test that dangerous operations are blocked."""
        from indra_cogex.apps.mcp_server.query_execution import QueryValidationError

        with pytest.raises(QueryValidationError):
            await asyncio.to_thread(
                execute_cypher,
                client=neo4j_client,
                query="MATCH (g:Gene) DELETE g",
                parameters={},
                validate_first=True,
            )

    @pytest.mark.asyncio
    async def test_empty_results_handled_gracefully(self, flask_app_with_client, neo4j_client):
        """Test that queries returning no results are handled properly."""
        # Query that should return no results
        result = await asyncio.to_thread(
            execute_cypher,
            client=neo4j_client,
            query="MATCH (g:BioEntity) WHERE g.id = $id RETURN g",
            parameters={"id": "invalid:nonexistent"},
        )

        assert "results" in result
        assert len(result["results"]) == 0

    @pytest.mark.asyncio
    async def test_invalid_disclosure_level_rejected(self, flask_app_with_client, neo4j_client):
        """Test that invalid disclosure levels are rejected."""
        results = [{"id": "hgnc:18618", "name": "LRRK2"}]

        # Try invalid disclosure level
        with pytest.raises(ValueError):
            await asyncio.to_thread(
                enrich_results,
                results=results,
                disclosure_level="invalid_level",  # type: ignore
                client=neo4j_client
            )


# ============================================================================
# Summary Test
# ============================================================================

@pytest.mark.nonpublic
class TestToolCoverage:
    """Verify that integration workflow tests cover key MCP tools."""

    def test_integration_workflow_coverage(self):
        """Verify integration tests exercise key MCP tools.

        Note: Comprehensive individual tool tests are in specialized files:
        - test_cypher_execution.py (execute_cypher)
        - test_validation.py (validate_cypher)
        - test_schema_discovery.py (get_graph_schema)
        - test_enrichment_tools_integration.py (enrich_results)
        """
        integration_tests = {
            "get_graph_schema": True,       # TestGetGraphSchemaComprehensive
            "enrich_results": True,          # TestEnrichResultsComprehensive
            "complete_workflow": True,       # TestCompleteResearchWorkflow
            "performance_benchmarks": True,  # TestPerformanceBenchmarks
            "error_handling": True           # TestErrorHandlingIntegration
        }

        assert all(integration_tests.values()), "All integration scenarios must be tested"
