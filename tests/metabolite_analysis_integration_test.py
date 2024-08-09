import unittest
import configparser
import os
import pandas as pd
import logging
from src.indra_cogex.analysis.metabolite_analysis import discrete_analysis, enzyme_analysis, metabolomics_ora
from src.indra_cogex.client.neo4j_client import Neo4jClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestMetaboliteAnalysisIntegration(unittest.TestCase):

    def test_database_content(self):
        logger.info("Checking database content")

        # Check for metabolites
        query = """
        MATCH (m:Metabolite)
        WHERE m.chebi_id IS NOT NULL
        RETURN count(m) as metabolite_count
        """
        result = self.client.query_tx(query)
        metabolite_count = result[0][0]  # Access using integer index
        logger.info(f"Number of metabolites in the database: {metabolite_count}")

        # Check for enzymes and their relationships
        query = """
        MATCH (e:Enzyme)-[:catalyzes]->(r:Reaction)-[:has_product]->(m:Metabolite)
        WHERE e.ec_code IS NOT NULL AND m.chebi_id IS NOT NULL
        RETURN count(DISTINCT e) as enzyme_count, count(DISTINCT m) as related_metabolite_count
        """
        result = self.client.query_tx(query)
        enzyme_count = result[0][0]  # Access using integer index
        related_metabolite_count = result[0][1]  # Access using integer index
        logger.info(f"Number of enzymes with related metabolites: {enzyme_count}")
        logger.info(f"Number of metabolites related to enzymes: {related_metabolite_count}")

        self.assertGreater(metabolite_count, 0, "No metabolites found in the database")
        self.assertGreater(enzyme_count, 0, "No enzymes with related metabolites found in the database")

    @classmethod
    def setUpClass(cls):
        config = configparser.ConfigParser()
        config.read(os.path.expanduser('~/.config/indra/config.ini'))

        neo4j_url = config.get('indra', 'INDRA_NEO4J_URL')
        neo4j_user = config.get('indra', 'INDRA_NEO4J_USER')
        neo4j_password = config.get('indra', 'INDRA_NEO4J_PASSWORD')

        cls.client = Neo4jClient(neo4j_url, auth=(neo4j_user, neo4j_password))
        logger.info("Connected to Neo4j database")

    def setUp(self):
        query = """
        MATCH (m:Metabolite)
        WHERE m.chebi_id IS NOT NULL
        RETURN m.chebi_id AS chebi_id, m.name AS name
        LIMIT 10
        """
        result = self.client.query_tx(query)
        self.real_metabolites = {row[0]: row[1] for row in result}  # Adjusted to use integer indices

        if not self.real_metabolites:
            logger.warning("No real metabolites found in the database.")
        else:
            logger.info(f"Retrieved {len(self.real_metabolites)} real metabolites from the database")

        self.test_metabolites = {
            **self.real_metabolites,
            "CHEBI:15377": "Water",
            "CHEBI:17234": "Glucose",
            "CHEBI:15343": "Acetate",
            "CHEBI:16828": "Pyruvate",
            "CHEBI:16761": "Lactate",
        }
        logger.info(f"Test metabolites: {self.test_metabolites}")

    def test_discrete_analysis(self):
        logger.info("Starting discrete_analysis test")
        for alpha in [0.05, 0.1, 0.2, 0.5]:
            result = discrete_analysis(
                self.client,
                self.test_metabolites,
                method='bonferroni',
                alpha=alpha,
                keep_insignificant=True,
                minimum_evidence_count=1,
                minimum_belief=0.5
            )

            self.assertIsNotNone(result)
            self.assertIn('results', result)

            logger.info(f"Number of pathways found with alpha={alpha}: {len(result['results'])}")
            if result['results']:
                for pathway_id, pathway_data in list(result['results'].items())[:5]:
                    logger.info(
                        f"Pathway: {pathway_data['name']}, p-value: {pathway_data['p_value']:.5f}, adjusted p-value: {pathway_data['adjusted_p_value']:.5f}")

            if len(result['results']) > 0:
                break

        self.assertGreater(len(result['results']), 0, "No significant pathways found with any tested alpha value")

    def test_enzyme_analysis(self):
        logger.info("Starting enzyme_analysis test")
        ec_codes_to_try = ['1.1.1.1', '2.7.1.1', '3.1.1.1', '4.1.1.1', '5.1.1.1']
        for ec_code in ec_codes_to_try:
            query = f"""
            MATCH (e:Enzyme{{ec_code:'{ec_code}'}})-[:catalyzes]->(r:Reaction)-[:has_product]->(m:Metabolite)
            WHERE m.chebi_id IS NOT NULL
            RETURN e.ec_code AS ec_code, collect(DISTINCT m.chebi_id) AS chebi_ids
            LIMIT 1
            """
            result = self.client.query_tx(query)
            if result:
                ec_code = result[0][0]  # Adjusted to use integer indices
                chebi_ids = result[0][1]  # Adjusted to use integer indices
                result = enzyme_analysis(
                    self.client,
                    ec_code=ec_code,
                    chebi_ids=chebi_ids
                )

                self.assertIsInstance(result, list)
                self.assertGreater(len(result), 0, f"No statements found for EC {ec_code}")
                logger.info(f"Number of statements found for EC {ec_code}: {len(result)}")
                for statement in result[:5]:
                    logger.info(f"Statement type: {statement.to_json()['type']}")
                return  # Test passes if we find results for any EC code

        self.fail("No suitable enzyme-metabolite pairs found for any tested EC code")

    def test_metabolomics_ora(self):
        logger.info("Starting metabolomics_ora test")
        try:
            chebi_ids = list(self.real_metabolites.keys())
            result = metabolomics_ora(
                client=self.client,
                chebi_ids=chebi_ids,
                method='bonferroni',
                alpha=0.05,
                minimum_belief=0.5
            )

            self.assertIsInstance(result, pd.DataFrame)
            if not result.empty:
                logger.info(f"Metabolomics ORA results shape: {result.shape}")
                logger.info(f"Columns: {result.columns.tolist()}")
                logger.info(f"First few rows:\n{result.head().to_string()}")
            else:
                logger.warning("Metabolomics ORA returned empty results")

        except Exception as e:
            logger.error(f"metabolomics_ora raised an exception: {str(e)}", exc_info=True)
            self.fail(f"metabolomics_ora raised an exception: {str(e)}")

    def test_discrete_analysis_with_real_data(self):
        logger.info("Starting discrete_analysis test with real data")
        try:
            result = discrete_analysis(
                self.client,
                self.real_metabolites,
                method='bonferroni',
                alpha=0.05,
                keep_insignificant=False,
                minimum_evidence_count=1,
                minimum_belief=0.5
            )

            self.assertIsNotNone(result)
            self.assertIn('results', result)
            self.assertIn('metabolites', result)

            logger.info(f"Number of input metabolites: {len(self.real_metabolites)}")
            logger.info(f"Number of pathways found: {len(result['results'])}")
            if result['results']:
                logger.info("Sample of results:")
                for curie, data in list(result['results'].items())[:5]:  # Print first 5 results
                    logger.info(
                        f"  {curie}: {data['name']} (p-value: {data['p_value']:.5f}, adjusted p-value: {data['adjusted_p_value']:.5f})")
            else:
                logger.warning("No significant pathways found.")

        except Exception as e:
            logger.error(f"discrete_analysis with real data raised an exception: {str(e)}", exc_info=True)
            self.fail(f"discrete_analysis with real data raised an exception: {str(e)}")


if __name__ == '__main__':
    unittest.main()
