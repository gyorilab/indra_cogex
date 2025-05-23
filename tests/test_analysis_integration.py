import json
import traceback
from pprint import pprint

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
from indra_cogex.apps.queries_web.cli import app
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
        "indra-upstream-kinases",
        "indra-upstream-tfs",
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
def test_kinase_analysis():
    """Integration test for kinase ORA functionality."""
    TEST_PHOSPHOSITES = [
        "RPS6KA1-S363", "RPS3-T42", "RPS6KA3-Y529",  # MAPK Signaling
        "RPS6KB1-S434", "RPS6-S244", "RPS6-S236",  # PI3K/AKT Signaling
        "RPA2-S29", "RPS6KB1-T412", "RNF8-T198",  # Cell Cycle
        "ROCK2-Y722", "BDKRB2-Y177"  # Tyrosine Kinases
    ]

    # Run analysis with fewer phosphosites
    result = kinase_analysis(
        phosphosite_list=TEST_PHOSPHOSITES,
        alpha=0.05,
        keep_insignificant=False  # Limit results to significant ones
    )

    assert isinstance(result, pd.DataFrame), "Result should be a DataFrame"
    assert not result.empty, "Result should not be empty"
    assert set(result.columns) >= {"curie", "name", "p", "q"}, "Missing expected columns"


@pytest.mark.nonpublic
def test_discrete_analysis_large_gene_list_indra_only():
    """Test discrete_analysis on large gene list for INDRA upstream/downstream with metadata."""
    payload = {
        "gene_list": [
            "613", "1116", "1119", "1697", "7067", "2537", "2734", "29517", "8568", "4910",
            "4931", "4932", "4962", "4983", "18873", "5432", "5433", "5981", "16404", "5985",
            "18358", "6018", "6019", "6021", "6118", "6120", "6122", "6148", "6374", "6378",
            "6395", "6727", "14374", "8004", "18669", "8912", "30306", "23785", "9253", "9788",
            "10498", "10819", "6769", "11120", "11133", "11432", "11584", "18348", "11849",
            "28948", "11876", "11878", "11985", "20820", "12647", "20593", "12713"
        ],
        "method": "fdr_bh",
        "alpha": 0.05,
        "keep_insignificant": False,
        "minimum_evidence_count": 1,
        "minimum_belief": 0.3,
        "indra_path_analysis": True,
        "background_gene_list": []
    }

    with app.test_client() as client:
        response = client.post(
            "/api/discrete_analysis",
            data=json.dumps(payload),
            content_type="application/json",
            headers={"Accept": "application/json"}
        )

        assert response.status_code == 200, f"Unexpected status: {response.status_code}"
        data = response.get_json()

        # ✅ Print upstream/downstream only
        print("\n\n📦 INDRA Upstream Output:")
        print(json.dumps(data.get("indra-upstream", []), indent=2))

        print("\n\n📦 INDRA Downstream Output:")
        print(json.dumps(data.get("indra-downstream", []), indent=2))

        # ✅ Validate INDRA results contain metadata
        for key in ["indra-upstream", "indra-downstream"]:
            if key in data and isinstance(data[key], list) and data[key]:
                assert "statements" in data[key][0], f"Missing 'statements' in {key}"
                assert isinstance(data[key][0]["statements"], list), f"'statements' in {key} is not a list"
                if data[key][0]["statements"]:
                    stmt = data[key][0]["statements"][0]
                    for field in ["gene", "stmt_hash", "belief", "evidence_count"]:
                        assert field in stmt, f"Missing field '{field}' in statement of {key}"


@pytest.mark.nonpublic
def test_discrete_analysis_metadata_debug_output():
    """Test discrete_analysis: print number of statements attached per enriched INDRA entity."""
    payload = {
        "gene_list": ["7157", "1956", "1950"],
        "method": "fdr_bh",
        "alpha": 0.05,
        "keep_insignificant": False,
        "minimum_evidence_count": 1,
        "minimum_belief": 0.3,
        "indra_path_analysis": True,
        "background_gene_list": []
    }

    with app.test_client() as client:
        response = client.post(
            "/api/discrete_analysis",
            data=json.dumps(payload),
            content_type="application/json",
            headers={"Accept": "application/json"}
        )

        assert response.status_code == 200, f"Unexpected status: {response.status_code}"
        data = response.get_json()

        # Only INDRA results
        for key in [
            "indra-upstream",
            "indra-downstream",
            "indra-upstream-kinases",
            "indra-upstream-tfs"
        ]:
            if key in data and isinstance(data[key], list):
                print(f"\n🔹 {key.upper()} ({len(data[key])} results):")
                for row in data[key]:
                    curie = row.get("curie")
                    stmts = row.get("statements", [])
                    print(f"  - {curie}: {len(stmts)} statements")
                    for s in stmts[:2]:  # Show up to 2 example statements
                        print(f"    ▶ {s}")


@pytest.mark.nonpublic
def test_signed_analysis_metadata_debug_output():
    """Test signed_analysis: print INDRA statement metadata for enriched results."""
    payload = {
        "positive_genes": [
            "10354", "4141", "1692", "11771", "4932", "12692", "6561", "3999",
            "20768", "10317", "5472", "10372", "12468", "132", "11253", "2198",
            "10304", "10383", "7406", "10401", "10388", "10386", "7028", "10410",
            "4933", "10333", "13312", "2705", "10336", "10610", "3189", "402",
            "11879", "8831", "10371", "2528", "17194", "12458", "11553", "11820"
        ],
        "negative_genes": [
            "5471", "11763", "2192", "2001", "17389", "3972", "10312", "8556",
            "10404", "7035", "7166", "13429", "29213", "6564", "6502", "15476",
            "13347", "20766", "3214", "13388", "3996", "7541", "10417", "4910",
            "2527", "667", "10327", "1546", "6492", "7", "163", "3284", "3774",
            "12437", "8547", "6908", "3218", "10424", "10496", "1595"
        ],
        "alpha": 0.05,
        "keep_insignificant": False,
        "minimum_evidence_count": 2,
        "minimum_belief": 0.7
    }

    with app.test_client() as client:
        response = client.post(
            "/api/signed_analysis",
            data=json.dumps(payload),
            content_type="application/json",
            headers={"Accept": "application/json"}
        )

        assert response.status_code == 200, f"Unexpected status: {response.status_code}"
        data = response.get_json()

        print("\n📦 SIGNED ANALYSIS OUTPUT:\n")
        if data and isinstance(data, list):
            formatted = json.dumps(data, indent=2)
            print(formatted)
        else:
            print("No results returned or unexpected format.")


@pytest.mark.nonpublic
def test_continuous_analysis_metadata_debug_output():
    payload = {
        "gene_names": ["YWHAZ", "YWHAQ", "PPP2R5A", "PPP2R5D", "PPP2R1A"],
        "log_fold_change": [0.05, -0.02, 0.1, -0.15, 0.08],
        "species": "human",
        "permutations": 100,
        "source": "indra-upstream",
        "alpha": 0.05,
        "keep_insignificant": False,
        "minimum_evidence_count": 1,
        "minimum_belief": 0.1
    }

    with app.test_client() as client, app.app_context():
        try:
            response = client.post(
                "/api/continuous_analysis",
                data=json.dumps(payload),
                content_type="application/json",
                headers={"Accept": "application/json"}
            )
            print("Response JSON:", response.get_json())
        except Exception as e:
            print("FULL TRACEBACK:")
            traceback.print_exc()
            raise

        assert response.status_code == 200, f"Unexpected status: {response.status_code}"


@pytest.mark.nonpublic
def test_kinase_analysis_metadata_output():
    """Test that kinase_analysis returns results with INDRA statement metadata."""
    payload = {
        "phosphosite_list": [
            "RPS6KA1-S363",
            "RPS3-T42",
            "RPS6KA3-Y529",
            "RPS6KB1-S434",
            "RPS6-S244",
            "RPS6-S236",
            "RPA2-S29",
            "RPS6KB1-T412",
            "RNF8-T198",
            "ROCK2-Y722",
            "BDKRB2-Y177",
            "BECN1-Y333"
        ],
        "alpha": 0.05,
        "keep_insignificant": False,
        "background": [],
        "minimum_evidence_count": 1,
        "minimum_belief": 0.1
    }

    with app.test_client() as client:
        response = client.post(
            "/api/kinase_analysis",
            data=json.dumps(payload),
            content_type="application/json",
            headers={"Accept": "application/json"}
        )

        assert response.status_code == 200, f"Unexpected status: {response.status_code}"
        data = response.get_json()
        assert isinstance(data, list), "Expected list of kinase analysis results"

        for row in data:
            assert "curie" in row and "statements" in row, "Missing expected keys"
            if row["statements"]:
                assert isinstance(row["statements"], list), "'statements' should be a list"
                for stmt in row["statements"]:
                    assert "stmt_hash" in stmt
                    assert "belief" in stmt
                    assert "evidence_count" in stmt
                    assert "gene" in stmt

        print("\n📦 KINASE ANALYSIS OUTPUT:")
        print(json.dumps(data, indent=2))
