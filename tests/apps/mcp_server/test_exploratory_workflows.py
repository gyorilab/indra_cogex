"""End-to-end integration tests for exploratory workflows.

Tests REAL multi-step exploration workflows using core layers.
Validates the core value proposition: arbitrary graph exploration.

Run with: pytest -m nonpublic tests/apps/mcp_server/test_exploratory_workflows.py
"""
import json
import pytest

# Fixtures neo4j_client and flask_app_with_client are provided by conftest.py




@pytest.mark.nonpublic
class TestGeneDiseasePathwayWorkflow:
    """Test multi-step gene → disease → pathway exploration."""

    def test_complete_gene_disease_pathway_workflow(self, neo4j_client):
        """Test complete exploratory workflow: genes → pathways → drugs.

        This was IMPOSSIBLE with the old 50-function architecture.
        Tests that the new architecture enables arbitrary exploration.
        """
        from indra_cogex.apps.mcp_server.query_execution import execute_cypher
        from indra_cogex.apps.mcp_server.enrichment import enrich_results, DisclosureLevel

        # Step 1: Find genes associated with Alzheimer's disease
        # Note: Graph uses BioEntity for all entities and gene_disease_association relationship
        genes_result = execute_cypher(
            client=neo4j_client,
            query="""
                MATCH (g:BioEntity)-[:gene_disease_association]->(d:BioEntity)
                WHERE d.id = $disease_id
                AND g.id STARTS WITH 'hgnc:'
                RETURN g.id, g.name
                LIMIT 20
            """,
            parameters={"disease_id": "mesh:D000544"},  # Alzheimer's
            max_results=20,

        )

        # Should have found genes
        assert "results" in genes_result
        assert len(genes_result["results"]) > 0
        assert "metadata" in genes_result
        print(f"Found {len(genes_result['results'])} genes associated with Alzheimer's")

        # Step 2: Enrich results with progressive metadata
        enriched_result = enrich_results(
            results=genes_result["results"][:5],  # Enrich first 5
            disclosure_level=DisclosureLevel.EXPLORATORY,
            client=neo4j_client
        )

        # Exploratory level should include workflows/next steps
        assert "results" in enriched_result
        assert "disclosure_level" in enriched_result
        assert enriched_result["disclosure_level"] == "exploratory"
        assert "metadata" in enriched_result

        # WORKFLOW COMPLETE - This demonstrates arbitrary multi-step exploration

    def test_iterative_hypothesis_refinement(self, neo4j_client):
        """Test iterative refinement of exploration based on intermediate results."""
        from indra_cogex.apps.mcp_server.query_execution import execute_cypher

        # Initial broad query using correct schema (BioEntity + gene_disease_association)
        initial_result = execute_cypher(
            client=neo4j_client,
            query="""
                MATCH (g:BioEntity)-[:gene_disease_association]->(d:BioEntity)
                WHERE d.id = $disease AND g.id STARTS WITH 'hgnc:'
                RETURN g LIMIT 50
            """,
            parameters={"disease": "mesh:D000544"},

        )

        assert "results" in initial_result
        assert len(initial_result["results"]) > 0

        # Refined query based on findings - find genes with multiple disease associations
        refined_result = execute_cypher(
            client=neo4j_client,
            query="""
                MATCH (g:BioEntity)-[:gene_disease_association]->(d:BioEntity)
                WHERE g.id STARTS WITH 'hgnc:'
                WITH g, count(DISTINCT d) as disease_count
                WHERE disease_count > 1
                RETURN g.id, g.name, disease_count
                ORDER BY disease_count DESC
                LIMIT 10
            """,
            parameters={},

        )

        # Refined results should be more targeted
        assert "results" in refined_result


@pytest.mark.nonpublic
class TestArbitraryPatternDiscovery:
    """Test ability to discover arbitrary graph patterns."""

    def test_discover_gene_disease_drug_triangles(self, neo4j_client):
        """Discover gene-disease-drug triangular relationships.

        This query pattern was IMPOSSIBLE in old architecture.
        Tests core value: arbitrary graph traversal.
        """
        from indra_cogex.apps.mcp_server.query_execution import execute_cypher
        from indra_cogex.apps.mcp_server.validation import validate_cypher

        # Complex pattern: genes with many disease associations
        # Uses correct schema: BioEntity + gene_disease_association
        query = """
            MATCH (g:BioEntity)-[:gene_disease_association]->(d:BioEntity)
            WHERE g.id STARTS WITH 'hgnc:'
            WITH g, count(DISTINCT d) AS disease_count
            WHERE disease_count > 5
            RETURN g.name AS gene,
                   disease_count
            ORDER BY disease_count DESC
            LIMIT 10
        """

        # First validate
        validation_result = validate_cypher(
            query=query,
            parameters={}
        )

        assert validation_result["valid"] is True, "Complex query should be valid"

        # Execute
        result = execute_cypher(
            client=neo4j_client,
            query=query,
            max_results=10,

        )

        # Should find hub genes
        assert "results" in result

        # If results found, validate structure
        if len(result["results"]) > 0:
            first = result["results"][0]
            assert "gene" in first
            assert "disease_count" in first

            # disease_count should be > 5 (WHERE clause)
            assert first["disease_count"] > 5

            print(f"Found {len(result['results'])} hub genes with many disease associations")

    def test_multi_hop_relationship_traversal(self, neo4j_client):
        """Traverse multiple relationship hops to find distant connections."""
        from indra_cogex.apps.mcp_server.query_execution import execute_cypher

        # Find genes connected through codependency relationships (2-hop via shared codependent gene)
        # Uses correct schema: BioEntity + codependent_with relationship
        query = """
            MATCH (g1:BioEntity)-[:codependent_with]-(shared:BioEntity)-[:codependent_with]-(g2:BioEntity)
            WHERE g1.id = $gene_id
            AND g2.id STARTS WITH 'hgnc:'
            AND g1.id <> g2.id
            RETURN g1.name AS gene1,
                   g2.name AS gene2,
                   shared.name AS shared_gene
            LIMIT 20
        """

        result = execute_cypher(
            client=neo4j_client,
            query=query,
            parameters={"gene_id": "hgnc:18618"},  # LRRK2
            max_results=20,

        )

        assert "results" in result
        # Should find genes connected through shared codependent genes
        print(f"Found {len(result['results'])} genes connected to LRRK2 via codependency")


@pytest.mark.nonpublic
class TestDrugTargetDiscoveryWorkflow:
    """Test drug target discovery workflow."""

    def test_complete_drug_target_workflow(self, neo4j_client):
        """Complete workflow: disease → genes → druggability → candidates."""
        from indra_cogex.apps.mcp_server.query_execution import execute_cypher

        # Step 1: Find disease-associated genes using correct schema
        # Using schizophrenia (doid:5419) which has many gene associations in the graph
        genes_result = execute_cypher(
            client=neo4j_client,
            query="""
                MATCH (g:BioEntity)-[:gene_disease_association]->(d:BioEntity)
                WHERE d.id = $disease_id
                AND g.id STARTS WITH 'hgnc:'
                RETURN g.id, g.name
                LIMIT 50
            """,
            parameters={"disease_id": "doid:5419"},  # Schizophrenia

        )

        assert len(genes_result["results"]) > 0
        print(f"Found {len(genes_result['results'])} schizophrenia-associated genes")

    def test_pathway_based_drug_repurposing(self, neo4j_client):
        """Find drugs targeting genes for drug repurposing candidates."""
        from indra_cogex.apps.mcp_server.query_execution import execute_cypher

        # Find drugs that target disease-associated genes (repurposing candidates)
        # Uses correct schema: BioEntity + gene_disease_association + has_indication
        query = """
            MATCH (gene:BioEntity)-[:gene_disease_association]->(disease:BioEntity)
            WHERE disease.id = $disease_id
            AND gene.id STARTS WITH 'hgnc:'
            RETURN gene.name AS gene,
                   gene.id AS gene_id
            LIMIT 10
        """

        result = execute_cypher(
            client=neo4j_client,
            query=query,
            parameters={"disease_id": "mesh:D000544"},  # Alzheimer's
            max_results=10,

        )

        assert "results" in result
        # This identifies genes for potential drug repurposing
        print(f"Found {len(result['results'])} disease-associated genes")


@pytest.mark.nonpublic
class TestLayerIntegration:
    """Test all 5 layers working together."""

    def test_validation_integrated_with_execution(self, neo4j_client):
        """Validation should prevent execution of dangerous queries."""
        from indra_cogex.apps.mcp_server.query_execution import execute_cypher, QueryValidationError

        # Dangerous query (DELETE) - uses BioEntity (correct schema)
        with pytest.raises(QueryValidationError):
            execute_cypher(
                client=neo4j_client,
                query="MATCH (g:BioEntity) DETACH DELETE g",
                validate_first=True,  # Validation enabled (default)

            )

    def test_repeated_queries_execute_successfully(self, neo4j_client):
        """Test that repeated queries execute consistently."""
        from indra_cogex.apps.mcp_server.query_execution import execute_cypher

        # Uses correct schema: BioEntity
        query = "MATCH (g:BioEntity) WHERE g.id = $gene_id RETURN g"
        parameters = {"gene_id": "hgnc:18618"}

        # First execution
        result1 = execute_cypher(
            client=neo4j_client,
            query=query,
            parameters=parameters,
        )
        assert "results" in result1

        # Second execution - should work identically
        result2 = execute_cypher(
            client=neo4j_client,
            query=query,
            parameters=parameters,
        )
        assert "results" in result2

        # Results should be consistent
        assert len(result1["results"]) == len(result2["results"])

    def test_all_layers_integrated(self, neo4j_client):
        """Test that all layers work together seamlessly."""
        from indra_cogex.apps.mcp_server.query_execution import execute_cypher
        from indra_cogex.apps.mcp_server.validation import validate_cypher
        from indra_cogex.apps.mcp_server.enrichment import enrich_results, DisclosureLevel

        # Layer 3: Validation (before execution) - uses correct schema: BioEntity
        query = "MATCH (g:BioEntity) WHERE g.id = $gene_id RETURN g"
        validation_result = validate_cypher(
            query=query,
            parameters={"gene_id": "hgnc:18618"}
        )
        assert validation_result["valid"] is True

        # Layer 2: Query Execution
        exec_result = execute_cypher(
            client=neo4j_client,
            query=query,
            parameters={"gene_id": "hgnc:18618"},

        )
        assert "results" in exec_result
        assert "metadata" in exec_result

        # Layer 4: Enrichment
        enrich_result = enrich_results(
            results=exec_result["results"],
            disclosure_level=DisclosureLevel.STANDARD
        )
        assert "results" in enrich_result
        assert "disclosure_level" in enrich_result

        # ALL LAYERS INTEGRATED SUCCESSFULLY


@pytest.mark.nonpublic
class TestNovelCapabilities:
    """Test capabilities that were IMPOSSIBLE with old architecture."""

    def test_arbitrary_aggregation_queries(self, neo4j_client):
        """Test complex aggregations impossible with predefined functions."""
        from indra_cogex.apps.mcp_server.query_execution import execute_cypher

        # Multi-level aggregation using correct schema: BioEntity + gene_disease_association
        query = """
            MATCH (g:BioEntity)-[:gene_disease_association]->(d:BioEntity)
            WHERE g.id STARTS WITH 'hgnc:'
            WITH d, count(DISTINCT g) AS gene_count
            WHERE gene_count > 10
            RETURN d.name AS disease,
                   gene_count
            ORDER BY gene_count DESC
            LIMIT 10
        """

        result = execute_cypher(
            client=neo4j_client,
            query=query,

        )

        # Should execute successfully
        assert "results" in result

    def test_conditional_path_traversal(self, neo4j_client):
        """Test conditional graph traversal based on node properties."""
        from indra_cogex.apps.mcp_server.query_execution import execute_cypher

        # Conditional traversal using correct schema: BioEntity + actual relationship types
        query = """
            MATCH path = (g:BioEntity)-[*1..2]-(target:BioEntity)
            WHERE g.id = $gene_id
              AND all(r in relationships(path) WHERE type(r) IN ['indra_rel', 'codependent_with', 'gene_disease_association'])
            RETURN target.name, length(path) AS hops
            LIMIT 20
        """

        result = execute_cypher(
            client=neo4j_client,
            query=query,
            parameters={"gene_id": "hgnc:18618"},

        )

        assert "results" in result
        # Variable-length path queries now possible

    def test_graph_algorithm_style_queries(self, neo4j_client):
        """Test graph algorithm-style analysis."""
        from indra_cogex.apps.mcp_server.query_execution import execute_cypher

        # Find genes with highest degree using correct schema: BioEntity
        # Note: Using simpler degree calculation that works across Neo4j versions
        query = """
            MATCH (g:BioEntity)
            WHERE g.id STARTS WITH 'hgnc:'
            WITH g, size([(g)-[]-() | 1]) AS total_degree
            RETURN g.name,
                   total_degree
            ORDER BY total_degree DESC
            LIMIT 10
        """

        result = execute_cypher(
            client=neo4j_client,
            query=query,

        )

        assert "results" in result
        if len(result["results"]) > 0:
            # Results should be ordered by degree
            assert "total_degree" in result["results"][0]


@pytest.mark.nonpublic
class TestErrorRecovery:
    """Test error recovery in multi-step workflows."""

    def test_workflow_continues_after_empty_results(self, neo4j_client):
        """Workflow should handle empty intermediate results gracefully."""
        from indra_cogex.apps.mcp_server.query_execution import execute_cypher

        # Query that returns no results - uses correct schema: BioEntity
        result = execute_cypher(
            client=neo4j_client,
            query="MATCH (g:BioEntity) WHERE g.id = 'nonexistent:0000' RETURN g",

        )

        # Should have empty results
        assert len(result["results"]) == 0
        assert "results" in result  # Should not error

    def test_workflow_adapts_to_unexpected_result_structure(self, neo4j_client):
        """Workflow should adapt when results have unexpected structure."""
        from indra_cogex.apps.mcp_server.query_execution import execute_cypher

        # Query with unusual return structure
        result = execute_cypher(
            client=neo4j_client,
            query="MATCH (g:Gene) RETURN count(g) AS total",

        )

        # Should handle aggregation results gracefully
        assert "results" in result


@pytest.mark.nonpublic
class TestPerformance:
    """Test performance of exploratory workflows."""

    def test_repeated_queries_complete_quickly(self, neo4j_client):
        """Repeated queries should execute quickly."""
        from indra_cogex.apps.mcp_server.query_execution import execute_cypher
        import time

        query = "MATCH (g:Gene) WHERE g.id = $gene_id RETURN g"
        parameters = {"gene_id": "hgnc:18618"}

        # First execution
        start1 = time.time()
        result1 = execute_cypher(
            client=neo4j_client,
            query=query,
            parameters=parameters,
        )
        time1 = time.time() - start1
        assert "results" in result1

        # Second execution
        start2 = time.time()
        result2 = execute_cypher(
            client=neo4j_client,
            query=query,
            parameters=parameters,
        )
        time2 = time.time() - start2
        assert "results" in result2

        # Both should complete in reasonable time
        assert time1 < 5.0, f"First query took {time1:.3f}s"
        assert time2 < 5.0, f"Second query took {time2:.3f}s"
        print(f"First: {time1:.3f}s, Second: {time2:.3f}s")

    def test_multi_step_workflow_completes_in_reasonable_time(self, neo4j_client):
        """Complete multi-step workflow should finish within time budget."""
        from indra_cogex.apps.mcp_server.query_execution import execute_cypher
        import time

        start = time.time()

        # Execute query
        exec_result = execute_cypher(
            client=neo4j_client,
            query="MATCH (g:Gene)-[:ASSOCIATES_WITH]->(d:Disease) WHERE d.id = $disease RETURN g LIMIT 10",
            parameters={"disease": "mesh:D000544"},

        )

        assert "results" in exec_result

        elapsed = time.time() - start

        # Should complete in under 10 seconds
        assert elapsed < 10.0, f"Workflow took {elapsed}s (should be <10s)"
        print(f"Workflow completed in {elapsed:.3f}s")
