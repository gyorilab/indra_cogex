import unittest
from unittest.mock import patch, Mock
from src.indra_cogex.analysis.gene_analysis import discrete_analysis
from src.indra_cogex.analysis.gene_analysis import signed_analysis


class TestDiscreteAnalysis(unittest.TestCase):
    def setUp(self):
        self.mock_client = Mock()
        self.test_genes = {f"HGNC:{i}": f"GENE{i}" for i in range(1, 31)}

        self.mock_ora_results = {
            "TERM:0000001": {"name": "Term 1", "p_value": 0.001, "adjusted_p_value": 0.005},
            "TERM:0000002": {"name": "Term 2", "p_value": 0.01, "adjusted_p_value": 0.05},
            "TERM:0000003": {"name": "Term 3", "p_value": 0.05, "adjusted_p_value": 0.25},
        }

    @patch('src.indra_cogex.analysis.gene_analysis.go_ora')
    @patch('src.indra_cogex.analysis.gene_analysis.wikipathways_ora')
    @patch('src.indra_cogex.analysis.gene_analysis.reactome_ora')
    @patch('src.indra_cogex.analysis.gene_analysis.phenotype_ora')
    @patch('src.indra_cogex.analysis.gene_analysis.indra_upstream_ora')
    @patch('src.indra_cogex.analysis.gene_analysis.indra_downstream_ora')
    @patch('src.indra_cogex.client.enrichment.discrete.count_human_genes', return_value=20000)
    def test_discrete_analysis(self, mock_count_human_genes, mock_indra_downstream_ora, mock_indra_upstream_ora,
                               mock_phenotype_ora, mock_reactome_ora, mock_wikipathways_ora, mock_go_ora):
        # Set up mock returns
        mock_go_ora.return_value = self.mock_ora_results
        mock_wikipathways_ora.return_value = self.mock_ora_results
        mock_reactome_ora.return_value = self.mock_ora_results
        mock_phenotype_ora.return_value = self.mock_ora_results
        mock_indra_upstream_ora.return_value = self.mock_ora_results
        mock_indra_downstream_ora.return_value = self.mock_ora_results

        result = discrete_analysis(
            self.mock_client,
            self.test_genes,
            method='bonferroni',
            alpha=0.05,
            keep_insignificant=True,
            minimum_evidence_count=1,
            minimum_belief=0.5
        )

        # Assert that all analysis types are present in the result
        self.assertIn('go_results', result)
        self.assertIn('wikipathways_results', result)
        self.assertIn('reactome_results', result)
        self.assertIn('phenotype_results', result)
        self.assertIn('indra_upstream_results', result)
        self.assertIn('indra_downstream_results', result)

        # Check results for each analysis type
        for analysis_type in result.keys():
            self.assertEqual(len(result[analysis_type]), 3)
            self.assertIn('TERM:0000001', result[analysis_type])
            self.assertEqual(result[analysis_type]['TERM:0000001']['name'], "Term 1")
            self.assertEqual(result[analysis_type]['TERM:0000001']['p_value'], 0.001)
            self.assertEqual(result[analysis_type]['TERM:0000001']['adjusted_p_value'], 0.005)

    @patch('src.indra_cogex.analysis.gene_analysis.go_ora')
    @patch('src.indra_cogex.analysis.gene_analysis.wikipathways_ora')
    @patch('src.indra_cogex.analysis.gene_analysis.reactome_ora')
    @patch('src.indra_cogex.analysis.gene_analysis.phenotype_ora')
    @patch('src.indra_cogex.analysis.gene_analysis.indra_upstream_ora')
    @patch('src.indra_cogex.analysis.gene_analysis.indra_downstream_ora')
    @patch('src.indra_cogex.client.enrichment.discrete.count_human_genes', return_value=20000)
    def test_discrete_analysis_keep_insignificant_false(self, mock_count_human_genes, mock_indra_downstream_ora,
                                                        mock_indra_upstream_ora,
                                                        mock_phenotype_ora, mock_reactome_ora, mock_wikipathways_ora,
                                                        mock_go_ora):
        # Set up mock returns with only significant results
        significant_results = {k: v for k, v in self.mock_ora_results.items() if v['adjusted_p_value'] <= 0.05}
        for mock_func in [mock_go_ora, mock_wikipathways_ora, mock_reactome_ora, mock_phenotype_ora,
                          mock_indra_upstream_ora, mock_indra_downstream_ora]:
            mock_func.return_value = significant_results

        result = discrete_analysis(
            self.mock_client,
            self.test_genes,
            method='bonferroni',
            alpha=0.05,
            keep_insignificant=False,
            minimum_evidence_count=1,
            minimum_belief=0.5
        )

        # Check that only significant results are kept
        for analysis_type in result.keys():
            self.assertEqual(len(result[analysis_type]), 2)
            self.assertIn('TERM:0000001', result[analysis_type])
            self.assertIn('TERM:0000002', result[analysis_type])
            self.assertNotIn('TERM:0000003', result[analysis_type])

    @patch('src.indra_cogex.analysis.gene_analysis.go_ora')
    @patch('src.indra_cogex.analysis.gene_analysis.wikipathways_ora')
    @patch('src.indra_cogex.analysis.gene_analysis.reactome_ora')
    @patch('src.indra_cogex.analysis.gene_analysis.phenotype_ora')
    @patch('src.indra_cogex.analysis.gene_analysis.indra_upstream_ora')
    @patch('src.indra_cogex.analysis.gene_analysis.indra_downstream_ora')
    @patch('src.indra_cogex.client.enrichment.discrete.count_human_genes', return_value=20000)
    def test_discrete_analysis_empty_gene_set(self, mock_count_human_genes, mock_indra_downstream_ora,
                                              mock_indra_upstream_ora,
                                              mock_phenotype_ora, mock_reactome_ora, mock_wikipathways_ora,
                                              mock_go_ora):
        # Set up mock returns for empty gene set
        empty_results = {}
        for mock_func in [mock_go_ora, mock_wikipathways_ora, mock_reactome_ora, mock_phenotype_ora,
                          mock_indra_upstream_ora, mock_indra_downstream_ora]:
            mock_func.return_value = empty_results

        result = discrete_analysis(
            self.mock_client,
            {},
            method='bonferroni',
            alpha=0.05,
            keep_insignificant=True,
            minimum_evidence_count=1,
            minimum_belief=0.5
        )

        # All result sets should be empty
        for analysis_type in result.keys():
            self.assertEqual(len(result[analysis_type]), 0)

    @patch('src.indra_cogex.analysis.gene_analysis.go_ora')
    @patch('src.indra_cogex.analysis.gene_analysis.wikipathways_ora')
    @patch('src.indra_cogex.analysis.gene_analysis.reactome_ora')
    @patch('src.indra_cogex.analysis.gene_analysis.phenotype_ora')
    @patch('src.indra_cogex.analysis.gene_analysis.indra_upstream_ora')
    @patch('src.indra_cogex.analysis.gene_analysis.indra_downstream_ora')
    @patch('src.indra_cogex.client.enrichment.discrete.count_human_genes', return_value=20000)
    def test_significant_results_only(self, mock_count_human_genes, mock_indra_downstream_ora, mock_indra_upstream_ora,
                                      mock_phenotype_ora, mock_reactome_ora, mock_wikipathways_ora, mock_go_ora):
        # Set up mock returns with varying p-values
        mock_go_ora.return_value = {
            'CURIE:001': {'name': 'Term 1', 'p_value': 0.001, 'adjusted_p_value': 0.005},
            'CURIE:002': {'name': 'Term 2', 'p_value': 0.01, 'adjusted_p_value': 0.05},
            'CURIE:003': {'name': 'Term 3', 'p_value': 0.05, 'adjusted_p_value': 0.25},
            'CURIE:004': {'name': 'Term 4', 'p_value': 0.1, 'adjusted_p_value': 0.5},
            'CURIE:005': {'name': 'Term 5', 'p_value': 0.5, 'adjusted_p_value': 1.0}
        }

        result = discrete_analysis(
            self.mock_client,
            self.test_genes,
            method='bonferroni',
            alpha=0.05,
            keep_insignificant=False,
            minimum_evidence_count=1,
            minimum_belief=0.5
        )

        # Check that only significant results (adjusted_p_value <= 0.05) are kept
        self.assertIn('go_results', result)
        significant_results = result['go_results']
        self.assertEqual(len(significant_results), 2, "Test: Significant results only: Unexpected number of results")
        self.assertIn('CURIE:001', significant_results)
        self.assertIn('CURIE:002', significant_results)
        self.assertNotIn('CURIE:003', significant_results)
        self.assertNotIn('CURIE:004', significant_results)
        self.assertNotIn('CURIE:005', significant_results)


if __name__ == '__main__':
    unittest.main()


class TestSignedAnalysis(unittest.TestCase):
    # Mock client class to simulate the behavior of the actual client
    class MockClient:
        @staticmethod
        def query(*args, **kwargs):
            return {
                "CURIE:001": {"Name": "Term 1", "genes": set(range(1, 21))},
                "CURIE:002": {"Name": "Term 2", "genes": set(range(11, 31))},
                "CURIE:003": {"Name": "Term 3", "genes": set(range(21, 41))},
                "CURIE:004": {"Name": "Term 4", "genes": set(range(31, 51))},
                "CURIE:005": {"Name": "Term 5", "genes": set(range(41, 61))}
            }

    # Mock function to simulate reverse causal reasoning
    @staticmethod
    def mock_reverse_causal_reasoning(client, positive_hgnc_ids, negative_hgnc_ids, *args, **kwargs):
        if not positive_hgnc_ids and not negative_hgnc_ids:
            return []
        elif not negative_hgnc_ids:
            return [
                {'id': 'CURIE:001', 'name': 'Term 1', 'correct': 15, 'incorrect': 0, 'ambiguous': 0, 'pvalue': 0.001},
                {'id': 'CURIE:002', 'name': 'Term 2', 'correct': 10, 'incorrect': 0, 'ambiguous': 5, 'pvalue': 0.05}
            ]
        elif not positive_hgnc_ids:
            return [
                {'id': 'CURIE:003', 'name': 'Term 3', 'correct': 0, 'incorrect': 15, 'ambiguous': 0, 'pvalue': 0.001},
                {'id': 'CURIE:004', 'name': 'Term 4', 'correct': 0, 'incorrect': 10, 'ambiguous': 5, 'pvalue': 0.05}
            ]
        else:
            return [
                {'id': 'CURIE:001', 'name': 'Term 1', 'correct': 15, 'incorrect': 5, 'ambiguous': 0, 'pvalue': 0.001},
                {'id': 'CURIE:002', 'name': 'Term 2', 'correct': 10, 'incorrect': 10, 'ambiguous': 0, 'pvalue': 0.5},
                {'id': 'CURIE:003', 'name': 'Term 3', 'correct': 5, 'incorrect': 15, 'ambiguous': 0, 'pvalue': 0.99},
                {'id': 'CURIE:004', 'name': 'Term 4', 'correct': 8, 'incorrect': 7, 'ambiguous': 5, 'pvalue': 0.1},
                {'id': 'CURIE:005', 'name': 'Term 5', 'correct': 0, 'incorrect': 0, 'ambiguous': 20, 'pvalue': None}
            ]

    # Helper method to run the signed analysis with mock data
    def run_signed_analysis(self, positive_genes, negative_genes, alpha, keep_insignificant):
        mock_client = self.MockClient()
        with patch('src.indra_cogex.analysis.gene_analysis.reverse_causal_reasoning',
                   side_effect=self.mock_reverse_causal_reasoning):
            return signed_analysis(
                mock_client,
                positive_genes,
                negative_genes,
                alpha=alpha,
                keep_insignificant=keep_insignificant,
                minimum_evidence_count=1,
                minimum_belief=0.5
            )

    # Helper method to assert the results
    def assert_results(self, result, expected_length, test_name):
        self.assertIn('results', result, f"{test_name}: 'results' key not found in output")
        self.assertIsInstance(result['results'], list, f"{test_name}: 'results' is not a list")
        self.assertEqual(len(result['results']), expected_length, f"{test_name}: Unexpected number of results")

    # Setup method to initialize common test data
    def setUp(self):
        self.positive_genes = {f"HGNC:{i}": f"GENE{i}" for i in range(1, 16)}
        self.negative_genes = {f"HGNC:{i}": f"GENE{i}" for i in range(16, 31)}

    # Test case 1: Default settings
    def test_default_settings(self):
        result = self.run_signed_analysis(self.positive_genes, self.negative_genes, alpha=0.05, keep_insignificant=True)
        self.assert_results(result, 5, "Test 1: Default settings")

    # Test case 2: Significant results only
    def test_significant_results_only(self):
        result = self.run_signed_analysis(self.positive_genes, self.negative_genes, alpha=0.05,
                                          keep_insignificant=False)
        self.assert_results(result, 3, "Test 2: Significant results only")

    # Test case 3: Empty input
    def test_empty_input(self):
        result = self.run_signed_analysis({}, {}, alpha=0.05, keep_insignificant=True)
        self.assert_results(result, 0, "Test 3: Empty input")

    # Test case 4: Only positive genes
    def test_only_positive_genes(self):
        result = self.run_signed_analysis(self.positive_genes, {}, alpha=0.05, keep_insignificant=True)
        self.assert_results(result, 2, "Test 4: Only positive genes")

    # Test case 5: Only negative genes
    def test_only_negative_genes(self):
        result = self.run_signed_analysis({}, self.negative_genes, alpha=0.05, keep_insignificant=True)
        self.assert_results(result, 2, "Test 5: Only negative genes")


# Main block to run the tests
if __name__ == '__main__':
    unittest.main()
