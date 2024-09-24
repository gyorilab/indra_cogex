import pytest
import pandas as pd
from typing import Dict

from indra_cogex.client.enrichment.discrete import EXAMPLE_GENE_IDS
from indra_cogex.client.neo4j_client import Neo4jClient
from indra_cogex.analysis.gene_analysis import discrete_analysis, signed_analysis


def test_discrete_analysis_frontend_defaults():
    # Tests example settings from frontend
    alpha = 0.05
    result = discrete_analysis(
        EXAMPLE_GENE_IDS,
        method='fdr_bh',  # Family-wise Correction with Benjamini/Hochberg
        alpha=alpha,
        keep_insignificant=False,
        minimum_evidence_count=1,
        minimum_belief=0.0,
        indra_path_analysis=False,
    )

    expected_analyses = {
        "go",
        "wikipathways",
        "reactome",
        "phenotype",
        "indra-upstream",
        "indra-downstream",
    }

    assert expected_analyses == set(result.keys()), "Result should have all expected analyses"

    # We don't run the INDRA analysis by default
    assert result["indra-upstream"] is None, "INDRA Upstream analysis should be None"
    assert result["indra-downstream"] is None, "INDRA Downstream analysis should be None"

    # Check that there are results and that all results are within the 0.05
    # significance level, since we're filtering out insignificant results with alpha=0.05
    for analysis_name, analysis_result in result.items():
        if analysis_result is None:
            assert analysis_name in ["indra-upstream", "indra-downstream"], \
                "Only INDRA analyses should be None"
        else:
            assert not analysis_result.empty, f"{analysis_name} result should not be empty"
            # Check p-values
            assert all(analysis_result["p"] <= alpha), \
                f"{analysis_name} should have all p-values <= 0.05"
            # Check corrected p-values (q)
            assert all(analysis_result["q"] <= alpha), \
                f"{analysis_name} should have all corrected p-values (q) <= 0.05"


def test_discrete_analysis_function_defaults():
    result = discrete_analysis(EXAMPLE_GENE_IDS)
    expected_analyses = {
        "go",
        "wikipathways",
        "reactome",
        "phenotype",
        "indra-upstream",
        "indra-downstream",
    }
    assert expected_analyses == set(
        result.keys()), "Result should have all expected analyses"

    # Check that there are result dataframes or None
    for analysis_name, analysis_result in result.items():
        assert analysis_result is None or not analysis_result.empty, \
            "Result should not be empty or None"

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

