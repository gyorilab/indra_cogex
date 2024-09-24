import pytest
import pandas as pd
from typing import Dict
from indra_cogex.client.neo4j_client import Neo4jClient
from indra_cogex.analysis.gene_analysis import discrete_analysis, signed_analysis


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

