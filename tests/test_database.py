import unittest
import configparser
import os
import logging
from src.indra_cogex.client.neo4j_client import Neo4jClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestDatabaseContent(unittest.TestCase):

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
        self.real_metabolites = {row[0]: row[1] for row in result}

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


if __name__ == '__main__':
    unittest.main()
