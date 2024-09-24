

import pytest
import pandas as pd
from typing import Dict
from indra_cogex.client.neo4j_client import Neo4jClient
from indra_cogex.analysis.gene_analysis import discrete_analysis, signed_analysis
from indra.config import get_config

# Get the Neo4j URL using INDRA's config reader
INDRA_NEO4J_URL = get_config("INDRA_NEO4J_URL")
print(f"Neo4j Connection URL: {INDRA_NEO4J_URL}")


@pytest.fixture(scope="module")
def neo4j_client() -> Neo4jClient:
    client = Neo4jClient()

    # Set timeout if possible
    if hasattr(client, 'set_timeout'):
        client.set_timeout(60)
    elif hasattr(client, 'driver') and hasattr(client.driver, 'set_timeout'):
        client.driver.set_timeout(60)

    return client


def test_neo4j_connection(neo4j_client: Neo4jClient):
    try:
        # Verify the connection
        assert neo4j_client.ping(), "Failed to ping Neo4j database"
    except Exception as e:
        pytest.fail(f"Failed to connect to Neo4j database: {str(e)}")


def get_random_genes(client: Neo4jClient, n: int = 10) -> Dict[str, str]:
    query = f"""
    MATCH (b:BioEntity)
    WHERE b.type = 'human_gene_protein'
    RETURN b.id, b.name
    LIMIT {n}
    """
    results = client.query_tx(query)
    genes = {row[0]: row[1] for row in results if len(row) == 2}
    return genes


def test_get_random_genes(neo4j_client: Neo4jClient):
    genes = get_random_genes(neo4j_client, 5)
    assert len(genes) > 0, "Should retrieve at least one gene"
    assert all(key.startswith('hgnc:') for key in genes.keys()), "All gene IDs should start with 'hgnc:'"


def get_sample_genes(client: Neo4jClient, limit: int = 10):
    query = """
    MATCH (g:BioEntity)
    WHERE g.type = 'human_gene_protein'
    RETURN g.id, g.name, g.type
    LIMIT $limit
    """
    results = client.query_tx(query, limit=limit)
    return results


def test_discrete_analysis_with_real_data(neo4j_client: Neo4jClient):
    genes = get_random_genes(neo4j_client, 100)

    result = discrete_analysis(
        genes,
        client=neo4j_client,
        method='fdr_bh',
        alpha=0.1,
        keep_insignificant=True,
        minimum_evidence_count=1,
        minimum_belief=0
    )

    assert isinstance(result, pd.DataFrame), "Result should be a DataFrame"
    if result.empty:
        pytest.skip("Result DataFrame is empty, skipping further assertions")
    assert "Analysis" in result.columns, "Result should have an 'Analysis' column"
    assert "p" in result.columns, "Result should have a 'p' column"
    expected_analyses = {"GO", "WikiPathways", "Reactome", "Phenotype", "INDRA Upstream", "INDRA Downstream"}
    assert not set(result['Analysis'].unique()).isdisjoint(expected_analyses), \
        "Result should contain at least one expected analysis type"


def test_signed_analysis_with_real_data(neo4j_client: Neo4jClient):
    all_genes = get_random_genes(neo4j_client, 80)

    # Split into positive and negative sets
    positive_genes = {gene_id: gene_name for gene_id, gene_name in list(all_genes.items())[:40]}
    negative_genes = {gene_id: gene_name for gene_id, gene_name in list(all_genes.items())[40:]}

    result = signed_analysis(
        positive_genes,
        negative_genes,
        client=neo4j_client,
        alpha=0.05,
        keep_insignificant=False,
        minimum_evidence_count=1,
        minimum_belief=0
    )

    assert isinstance(result, pd.DataFrame), "Result should be a DataFrame"
    if result.empty:
        pytest.skip("Result DataFrame is empty, skipping further assertions")
    expected_columns = {"curie", "name", "correct", "incorrect", "ambiguous", "binom_pvalue"}
    assert not expected_columns.isdisjoint(
        result.columns), f"Result should have at least one of these columns: {expected_columns}"

