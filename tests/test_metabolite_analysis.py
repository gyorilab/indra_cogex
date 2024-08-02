import unittest
from unittest.mock import patch, Mock
from src.indra_cogex.analysis.metabolite_analysis import discrete_analysis, enzyme_analysis


class TestMetaboliteAnalysis(unittest.TestCase):

    def setUp(self):
        self.mock_client = Mock()
        self.test_metabolites = {
            "CHEBI:15377": "Water",
            "CHEBI:17234": "Glucose",
            "CHEBI:15343": "Acetate",
            "CHEBI:16828": "Pyruvate",
            "CHEBI:16761": "Lactate",
        }

    @patch('src.indra_cogex.analysis.metabolite_analysis.metabolomics_ora')
    def test_discrete_analysis_multiple_pathways(self, mock_metabolomics_ora):
        mock_metabolomics_ora.return_value = {
            "KEGG:hsa00010": {"name": "Glycolysis / Gluconeogenesis", "p_value": 0.001, "adjusted_p_value": 0.005,
                              "evidence_count": 10},
            "KEGG:hsa00020": {"name": "Citrate cycle (TCA cycle)", "p_value": 0.01, "adjusted_p_value": 0.05,
                              "evidence_count": 8},
            "KEGG:hsa00030": {"name": "Pentose phosphate pathway", "p_value": 0.05, "adjusted_p_value": 0.25,
                              "evidence_count": 6},
            "KEGG:hsa00620": {"name": "Pyruvate metabolism", "p_value": 0.02, "adjusted_p_value": 0.1,
                              "evidence_count": 7}
        }

        result = discrete_analysis(
            self.mock_client,
            self.test_metabolites,
            method='bonferroni',
            alpha=0.05,
            keep_insignificant=False,
            minimum_evidence_count=1,
            minimum_belief=0.5
        )

        self.assertEqual(len(result['results']), 2)
        self.assertIn('KEGG:hsa00010', result['results'])
        self.assertIn('KEGG:hsa00020', result['results'])

    @patch('src.indra_cogex.analysis.metabolite_analysis.metabolomics_ora')
    def test_discrete_analysis_different_alpha(self, mock_metabolomics_ora):
        mock_metabolomics_ora.return_value = {
            "KEGG:hsa00010": {"name": "Glycolysis / Gluconeogenesis", "p_value": 0.001, "adjusted_p_value": 0.005,
                              "evidence_count": 10},
            "KEGG:hsa00020": {"name": "Citrate cycle (TCA cycle)", "p_value": 0.01, "adjusted_p_value": 0.05,
                              "evidence_count": 8},
            "KEGG:hsa00030": {"name": "Pentose phosphate pathway", "p_value": 0.05, "adjusted_p_value": 0.25,
                              "evidence_count": 6}
        }

        result = discrete_analysis(
            self.mock_client,
            self.test_metabolites,
            method='bonferroni',
            alpha=0.01,
            keep_insignificant=False,
            minimum_evidence_count=1,
            minimum_belief=0.5
        )

        print(f"Test result: {result}")
        self.assertEqual(len(result['results']), 1)
        self.assertIn('KEGG:hsa00010', result['results'])

    @patch('src.indra_cogex.analysis.metabolite_analysis.metabolomics_ora')
    def test_discrete_analysis_different_correction_method(self, mock_metabolomics_ora):
        mock_metabolomics_ora.return_value = {
            "KEGG:hsa00010": {"name": "Glycolysis / Gluconeogenesis", "p_value": 0.001, "adjusted_p_value": 0.003,
                              "evidence_count": 10},
            "KEGG:hsa00020": {"name": "Citrate cycle (TCA cycle)", "p_value": 0.01, "adjusted_p_value": 0.03,
                              "evidence_count": 8},
            "KEGG:hsa00030": {"name": "Pentose phosphate pathway", "p_value": 0.05, "adjusted_p_value": 0.15,
                              "evidence_count": 6}
        }

        result = discrete_analysis(
            self.mock_client,
            self.test_metabolites,
            method='fdr_bh',
            alpha=0.05,
            keep_insignificant=False,
            minimum_evidence_count=1,
            minimum_belief=0.5
        )

        self.assertEqual(len(result['results']), 2)
        self.assertIn('KEGG:hsa00010', result['results'])
        self.assertIn('KEGG:hsa00020', result['results'])

    @patch('src.indra_cogex.analysis.metabolite_analysis.metabolomics_explanation')
    def test_enzyme_analysis_multiple_enzymes(self, mock_metabolomics_explanation):
        mock_statements = [
            Mock(to_json=lambda: {"type": "Statement1", "content": "Enzyme catalyzes reaction X"}),
            Mock(to_json=lambda: {"type": "Statement2", "content": "Enzyme is involved in pathway Y"}),
            Mock(to_json=lambda: {"type": "Statement3", "content": "Enzyme regulates metabolite Z"})
        ]
        mock_metabolomics_explanation.return_value = mock_statements

        result = enzyme_analysis(
            self.mock_client,
            ec_code="1.1.1.1",
            chebi_ids=["CHEBI:15377", "CHEBI:17234", "CHEBI:15422"]
        )

        self.assertEqual(len(result), 3)
        self.assertEqual(result[2].to_json()["type"], "Statement3")

    def test_enzyme_analysis_no_chebi_ids(self):
        mock_statement = Mock()
        mock_statement.to_json.return_value = {"type": "Statement", "content": "Test"}
        self.mock_client.query_tx.return_value = iter([('[{"type": "Statement", "content": "Test"}]',)])

        with patch('src.indra_cogex.analysis.metabolite_analysis.metabolomics_explanation',
                   return_value=[mock_statement]):
            result = enzyme_analysis(
                self.mock_client,
                ec_code="2.7.1.1"
            )

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].to_json(), {"type": "Statement", "content": "Test"})

    @patch('src.indra_cogex.analysis.metabolite_analysis.metabolomics_ora')
    def test_discrete_analysis_minimum_evidence_count(self, mock_metabolomics_ora):
        mock_metabolomics_ora.return_value = {
            "KEGG:hsa00010": {"name": "Glycolysis / Gluconeogenesis", "p_value": 0.001, "adjusted_p_value": 0.005,
                              "evidence_count": 10},
            "KEGG:hsa00020": {"name": "Citrate cycle (TCA cycle)", "p_value": 0.01, "adjusted_p_value": 0.05,
                              "evidence_count": 5},
            "KEGG:hsa00030": {"name": "Pentose phosphate pathway", "p_value": 0.05, "adjusted_p_value": 0.25,
                              "evidence_count": 3}
        }

        result = discrete_analysis(
            self.mock_client,
            self.test_metabolites,
            method='bonferroni',
            alpha=0.05,
            keep_insignificant=True,
            minimum_evidence_count=6,
            minimum_belief=0.5
        )

        self.assertEqual(len(result['results']), 1)
        self.assertIn('KEGG:hsa00010', result['results'])

    @patch('src.indra_cogex.analysis.metabolite_analysis.metabolomics_ora')
    def test_discrete_analysis_empty_input(self, mock_metabolomics_ora):
        mock_metabolomics_ora.return_value = {}

        result = discrete_analysis(
            self.mock_client,
            {},
            method='bonferroni',
            alpha=0.05,
            keep_insignificant=True,
            minimum_evidence_count=1,
            minimum_belief=0.5
        )

        self.assertEqual(result['metabolites'], {})
        self.assertEqual(result['results'], {})

    @patch('src.indra_cogex.analysis.metabolite_analysis.metabolomics_ora')
    def test_discrete_analysis_all_insignificant(self, mock_metabolomics_ora):
        mock_metabolomics_ora.return_value = {
            "KEGG:hsa00010": {"name": "Glycolysis / Gluconeogenesis", "p_value": 0.1, "adjusted_p_value": 0.5,
                              "evidence_count": 10},
            "KEGG:hsa00020": {"name": "Citrate cycle (TCA cycle)", "p_value": 0.2, "adjusted_p_value": 0.6,
                              "evidence_count": 8},
            "KEGG:hsa00030": {"name": "Pentose phosphate pathway", "p_value": 0.3, "adjusted_p_value": 0.7,
                              "evidence_count": 6}
        }

        result = discrete_analysis(
            self.mock_client,
            self.test_metabolites,
            method='bonferroni',
            alpha=0.05,
            keep_insignificant=False,
            minimum_evidence_count=1,
            minimum_belief=0.5
        )

        self.assertEqual(len(result['results']), 0)


if __name__ == '__main__':
    unittest.main()
