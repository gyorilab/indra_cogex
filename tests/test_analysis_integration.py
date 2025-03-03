import pandas as pd
import pytest

from indra.statements import Statement
from indra_cogex.analysis import gene_continuous_analysis_example_data
from indra_cogex.analysis.gene_analysis import (
    discrete_analysis,
    signed_analysis,
    continuous_analysis, kinase_analysis
)
from indra_cogex.analysis.metabolite_analysis import (
    metabolite_discrete_analysis,
    enzyme_analysis
)
from indra_cogex.client import Neo4jClient
from indra_cogex.client.enrichment.discrete import EXAMPLE_GENE_IDS, count_phosphosites
from indra_cogex.client.enrichment.mla import EXAMPLE_CHEBI_CURIES
from indra_cogex.client.enrichment.signed import (
    EXAMPLE_POSITIVE_HGNC_IDS,
    EXAMPLE_NEGATIVE_HGNC_IDS
)


@pytest.mark.nonpublic
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
    }

    assert expected_analyses == set(result.keys()), "Result should have all expected analyses"

    # We don't run the INDRA analysis by default
    assert "indra-upstream" not in result, "INDRA Upstream analysis should not be in result"
    assert "indra-downstream" not in result, "INDRA Downstream analysis should not be in result"

    # Check that there are results and that all results are within the 0.05
    # significance level, since we're filtering out insignificant results with alpha=0.05
    for analysis_name, analysis_result in result.items():
        assert analysis_result is not None, f"{analysis_name} result should not be None"
        assert not analysis_result.empty, f"{analysis_name} result should not be empty"
        # Check p-values
        assert all(analysis_result["p"] <= alpha), \
            f"{analysis_name} should have all p-values <= 0.05"


@pytest.mark.nonpublic
def test_discrete_analysis_with_indra():
    # Tests example settings from frontend
    alpha = 0.05
    result = discrete_analysis(
        EXAMPLE_GENE_IDS,
        method='fdr_bh',  # Family-wise Correction with Benjamini/Hochberg
        alpha=alpha,
        keep_insignificant=False,
        minimum_evidence_count=2,
        minimum_belief=0.7,
        indra_path_analysis=True,
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

    # Check that there are results and that all results are within the 0.05
    # significance level, since we're filtering out insignificant results with alpha=0.05
    for analysis_name, analysis_result in result.items():
        assert analysis_result is not None, f"{analysis_name} result should not be None"
        assert not analysis_result.empty, f"{analysis_name} result should not be empty"
        # Check p-values
        assert all(analysis_result["p"] <= alpha), \
            f"{analysis_name} should have all p-values <= 0.05"


@pytest.mark.nonpublic
def test_discrete_analysis_function_defaults():
    result = discrete_analysis(EXAMPLE_GENE_IDS)
    expected_analyses = {
        "go",
        "wikipathways",
        "reactome",
        "phenotype",
    }
    assert expected_analyses == set(result.keys()), "Result should have all expected analyses"

    # Check that there are result dataframes or None
    for analysis_name, analysis_result in result.items():
        assert analysis_result is not None, "Result should not be None"
        assert not analysis_result.empty, "Result should not be empty"


@pytest.mark.nonpublic
def test_signed_analysis_frontend_defaults():
    # Test example settings from frontend
    alpha = 0.05
    result = signed_analysis(
        EXAMPLE_POSITIVE_HGNC_IDS,
        EXAMPLE_NEGATIVE_HGNC_IDS,
        alpha=alpha,
        keep_insignificant=False,
        minimum_evidence_count=1,
        minimum_belief=0
    )

    assert result is not None, "Result should not be None"
    assert isinstance(result, pd.DataFrame), "Result should be a DataFrame"
    assert not result.empty, "Result should not be empty"
    assert (result["binom_pvalue"] <= alpha).all(), "All p-values should be <= 0.05"


@pytest.mark.nonpublic
def test_signed_analysis_function_defaults():
    # Test defaults from function
    result = signed_analysis(
        EXAMPLE_POSITIVE_HGNC_IDS,
        EXAMPLE_NEGATIVE_HGNC_IDS,
    )

    assert result is not None, "Result should not be None"
    assert isinstance(result, pd.DataFrame), "Result should be a DataFrame"
    assert not result.empty, "Result should not be empty"


@pytest.mark.nonpublic
def test_continuous_analysis_with_frontend_defaults():
    test_data_df = pd.read_csv(gene_continuous_analysis_example_data)
    alpha = 0.05

    result = continuous_analysis(
        gene_names=test_data_df['gene_name'].values,
        log_fold_change=test_data_df['log2FoldChange'].values,
        species="human",
        permutations=100,
        source="go",
        alpha=alpha,
        keep_insignificant=False,
        minimum_evidence_count=1,
        minimum_belief=0.0
    )

    assert result is not None, "Result should not be None"
    assert isinstance(result, pd.DataFrame), "Result should be a DataFrame"
    assert not result.empty, "Result should not be empty"
    assert (result["NOM p-val"] <= alpha).all(), "All corrected p-values should be <= 0.05"


@pytest.mark.nonpublic
def test_continuous_analysis_with_function_defaults():
    test_data_df = pd.read_csv(gene_continuous_analysis_example_data)

    result = continuous_analysis(
        gene_names=test_data_df['gene_name'].values,
        log_fold_change=test_data_df['log2FoldChange'].values,
        species="human",
        permutations=100,
        source="go"
    )

    assert result is not None, "Result should not be None"
    assert isinstance(result, pd.DataFrame), "Result should be a DataFrame"
    assert not result.empty, "Result should not be empty"


@pytest.mark.nonpublic
def test_metabolite_analysis_frontend_defaults():
    alpha = 0.05
    result = metabolite_discrete_analysis(
        metabolites=EXAMPLE_CHEBI_CURIES,
        method="fdr_bh",
        alpha=alpha,
        keep_insignificant=False,
        minimum_evidence_count=1,
        minimum_belief=0.0
    )

    assert result is not None, "Result should not be None"
    assert isinstance(result, pd.DataFrame), "Result should be a DataFrame"
    assert not result.empty, "Result should not be empty"
    assert (result["q"] <= alpha).all(), "All q-values should be <= 0.05"


@pytest.mark.nonpublic
def test_metabolite_analysis_function_defaults():
    result = metabolite_discrete_analysis(EXAMPLE_CHEBI_CURIES)

    assert result is not None, "Result should not be None"
    assert isinstance(result, pd.DataFrame), "Result should be a DataFrame"
    assert not result.empty, "Result should not be empty"


@pytest.mark.nonpublic
def test_enzyme_analysis():
    res = enzyme_analysis(ec_code="1.1.1.1")
    assert isinstance(res, list), "Result should be a list"
    assert all(isinstance(s, Statement) for s in res), "All results should be INDRA Statements"
    assert len(res) > 0, "Result should not be empty"


@pytest.mark.nonpublic
def test_kinase_analysis_integration():
    """Integration test for the kinase ORA functionality using a comprehensive set of phosphosites."""
    # Use a comprehensive set of phosphosites from major kinases
    TEST_PHOSPHOSITES = [
        'RUNX1-S21', 'RPS6-S6', 'SDC1-Y286', 'RPS6KA3-S19', 'RPS3A-T88',
        'RPS3A-S112', 'BMX-Y566', 'RNMT-T77', 'RPS6KB1-T412', 'SDC2-Y179',
        'RPS6KB2-S356', 'RUNX2-S28', 'RPS6KA3-Y580', 'RUNX1-S266', 'BLM-S714',
        'BCL9-T172', 'RPL13-S106', 'RPS6KB1-T399', 'STMN2-S73', 'SFPQ-Y691',
        'RIPK1-Y384', 'ROCK2-Y722', 'BDKRB2-Y347', 'BDKRB2-Y177', 'RPA2-S33',
        'RPA2-S29', 'BECN1-S295', 'BECN1-S234'
        # Add more from your comprehensive set as needed
    ]

    print(f"\nRunning kinase analysis with {len(TEST_PHOSPHOSITES)} phosphosites")

    # Run analysis with the comprehensive phosphosite set
    result = kinase_analysis(
        phosphosite_list=TEST_PHOSPHOSITES,
        alpha=0.05,
        keep_insignificant=True
    )

    # Display results
    print(f"\nAnalysis results: {result.shape[0]} rows")
    if not result.empty:
        # Sort by p-value for better readability
        sorted_result = result.sort_values('p')
        print("\nTop 20 enriched kinases:")
        print(sorted_result.head(20))

        # Count significant kinases
        significant_count = (sorted_result['p'] < 0.05).sum()
        print(f"\nFound {significant_count} kinases with p < 0.05")

    # Basic validation
    assert isinstance(result, pd.DataFrame), "Result should be a DataFrame"
    assert not result.empty, "Result should not be empty with valid phosphosites"

    # Check expected columns
    expected_columns = {"curie", "name", "p", "q", "mlp", "mlq"}
    assert set(result.columns) >= expected_columns, "Result should have all expected columns"
