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

    @classmethod
    def setUpClass(cls):
        config = configparser.ConfigParser()
        config.read(os.path.expanduser('~/.config/indra/config.ini'))

        neo4j_url = config.get('indra', 'INDRA_NEO4J_URL')
        neo4j_user = config.get('indra', 'INDRA_NEO4J_USER')
        neo4j_password = config.get('indra', 'INDRA_NEO4J_PASSWORD')

        cls.client = Neo4jClient(neo4j_url, auth=(neo4j_user, neo4j_password))

    def setUp(self):
        query = """
        MATCH (m:BioEntity)
        WHERE m.id STARTS WITH 'chebi'
        RETURN m.id AS chebi_id, m.name AS name
        LIMIT 10
        """
        result = self.client.query_tx(query)
        self.real_metabolites = {row[0]: row[1] for row in result if row[0] and row[1]}

        if not self.real_metabolites:
            logger.warning("No real metabolites found in the database.")

        self.test_metabolites = {
            **self.real_metabolites,
            "CHEBI:15377": "Water",
            "CHEBI:17234": "Glucose",
            "CHEBI:15343": "Acetate",
            "CHEBI:16828": "Pyruvate",
            "CHEBI:16761": "Lactate",
        }

    def test_database_content(self):
        # Check for metabolites
        query = """
        MATCH (m:BioEntity)
        WHERE m.id STARTS WITH 'chebi:'
        RETURN count(m) as metabolite_count
        """
        result = self.client.query_tx(query)
        metabolite_count = result[0][0] if result else 0

        # Check for enzymes
        query = """
        MATCH (e:BioEntity)
        WHERE e.id STARTS WITH 'ec-code:'
        RETURN count(e) as enzyme_count
        """
        result = self.client.query_tx(query)
        enzyme_count = result[0][0] if result else 0

        self.assertGreater(metabolite_count, 0, "No metabolites found in the database")
        self.assertGreater(enzyme_count, 0, "No enzymes found in the database")

    def test_discrete_analysis(self):
        for alpha in [0.05, 0.1, 0.2, 0.5, 1.0]:
            result = discrete_analysis(
                metabolites=self.test_metabolites,
                method='bonferroni',
                alpha=alpha,
                keep_insignificant=True,
                minimum_evidence_count=1,
                minimum_belief=0.5,
                client=self.client
            )
            self.assertIsNotNone(result)
            self.assertIn('results', result)

            if result['results']:
                for pathway_id, pathway_data in list(result['results'].items())[:5]:
                    logger.info(
                        f"Pathway: {pathway_data['name']}, p-value: {pathway_data['p_value']:.5f}")

                break

    def test_node_content(self):
        # Check a metabolite
        query = "MATCH (m:BioEntity) WHERE m.id STARTS WITH 'chebi:' RETURN m LIMIT 1"
        result = self.client.query_tx(query)

        # Check an enzyme
        query = "MATCH (e:BioEntity) WHERE e.id STARTS WITH 'ec-code:' RETURN e LIMIT 1"
        result = self.client.query_tx(query)

    def test_enzyme_metabolite_relationships(self):
        query = """
        MATCH (e:BioEntity)-[r]->(m:BioEntity)
        WHERE e.id STARTS WITH 'ec-code:' AND m.id STARTS WITH 'chebi:'
        RETURN type(r) AS relationship_type, count(*) AS count
        LIMIT 5
        """
        result = self.client.query_tx(query)
        self.assertTrue(len(result) > 0, "No relationships found between enzymes and metabolites")

    def test_enzyme_analysis(self):
        query = """
        MATCH (e:BioEntity)
        WHERE e.id STARTS WITH 'ec-code:'
        RETURN e.id AS ec_code
        LIMIT 5
        """
        result = self.client.query_tx(query)

        if not result:
            logger.warning("No enzymes found in the database. Skipping enzyme analysis test.")
            return

        ec_codes_to_try = [row[0] for row in result]

        for ec_code in ec_codes_to_try:
            query = f"""
            MATCH (e:BioEntity{{id:'{ec_code}'}})-[r]->(m:BioEntity)
            WHERE m.id STARTS WITH 'chebi:'
            RETURN e.id AS ec_code, collect(DISTINCT m.id) AS chebi_ids, collect(DISTINCT type(r)) AS relationship_types
            """
            result = self.client.query_tx(query)

            if result and result[0][1]:  # Check if chebi_ids is not empty
                ec_code = result[0][0]
                chebi_ids = result[0][1]

                result = enzyme_analysis(
                    self.client,
                    ec_code=ec_code.replace('ec-code:', ''),
                    chebi_ids=chebi_ids
                )

                self.assertIsInstance(result, list)
                self.assertGreater(len(result), 0, f"No statements found for EC {ec_code}")

                return  # Test passes if we find results for any EC code

        logger.warning("No suitable enzyme-metabolite pairs found for any tested EC code")
        self.skipTest("No suitable enzyme-metabolite pairs found for any tested EC code")

    def test_metabolomics_ora(self):
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

        except Exception as e:
            logger.error(f"metabolomics_ora raised an exception: {str(e)}", exc_info=True)
            self.fail(f"metabolomics_ora raised an exception: {str(e)}")

    def test_discrete_analysis_with_real_data(self):
        try:
            result = discrete_analysis(
                metabolites=self.test_metabolites,
                method='bonferroni',
                alpha=0.05,
                keep_insignificant=True,
                minimum_evidence_count=1,
                minimum_belief=0.5,
                client=self.client
            )

            self.assertIsInstance(result, pd.DataFrame)
            self.assertFalse(result.empty, "Result DataFrame is empty")
            expected_columns = ['curie', 'name', 'p', 'adjusted_p_value', 'evidence_count']
            self.assertTrue(all(col in result.columns for col in expected_columns),
                            f"Result DataFrame is missing expected columns. Columns: {result.columns}")

            logger.info(f"Number of input metabolites: {len(self.real_metabolites)}")
            logger.info(f"Number of pathways found: {len(result)}")
            if not result.empty:
                logger.info("Sample of results:")
                logger.info(result.head().to_string())
            else:
                logger.warning("No significant pathways found.")

        except Exception as e:
            logger.error(f"discrete_analysis with real data raised an exception: {str(e)}", exc_info=True)
            self.fail(f"discrete_analysis with real data raised an exception: {str(e)}")

    def test_node_existence(self):
        enzyme_query = "MATCH (e:BioEntity) WHERE e.id STARTS WITH 'ec-code:' RETURN COUNT(e) as count"
        metabolite_query = "MATCH (m:BioEntity) WHERE m.id STARTS WITH 'chebi:' RETURN COUNT(m) as count"

        enzyme_count = self.client.query_tx(enzyme_query)[0]['count']
        metabolite_count = self.client.query_tx(metabolite_query)[0]['count']

        logger.info(f"Enzyme count: {enzyme_count}")
        logger.info(f"Metabolite count: {metabolite_count}")

        self.assertGreater(enzyme_count, 0, "No enzyme nodes found")
        self.assertGreater(metabolite_count, 0, "No metabolite nodes found")

    def test_relationship_types(self):
        query = """
           MATCH (e:BioEntity)-[r]->(m:BioEntity)
           WHERE e.id STARTS WITH 'ec-code:' OR m.id STARTS WITH 'chebi:'
           RETURN DISTINCT type(r) AS relationship_type
           """
        result = self.client.query_tx(query)
        logger.info(f"Relationship types: {result}")
        self.assertTrue(len(result) > 0, "No relationships found involving enzymes or metabolites")

    def test_sample_nodes(self):
        enzyme_query = "MATCH (e:BioEntity) WHERE e.id STARTS WITH 'ec-code:' RETURN e LIMIT 1"
        metabolite_query = "MATCH (m:BioEntity) WHERE m.id STARTS WITH 'chebi:' RETURN m LIMIT 1"

        enzyme = self.client.query_tx(enzyme_query)
        metabolite = self.client.query_tx(metabolite_query)

        logger.info(f"Sample enzyme node: {enzyme}")
        logger.info(f"Sample metabolite node: {metabolite}")


if __name__ == '__main__':
    unittest.main()
