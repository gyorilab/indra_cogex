import argparse
import pickle

from indra_cogex.apps.constants import AGENT_NAME_CACHE
from indra_cogex.client import Neo4jClient


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Utility for search-related tasks.")
    parser.add_argument(
        "--cache", action="store_true",
        help="Load and save agent cache to file."
    )
    args = parser.parse_args()
    if args.cache:
        neo4j_client = Neo4jClient()
        agent_cache = neo4j_client.load_agent_cache()
        with open(AGENT_NAME_CACHE, "wb") as f:
            pickle.dump(agent_cache, f)
