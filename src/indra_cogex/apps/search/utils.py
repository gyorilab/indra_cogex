import argparse
import csv
import gzip
import logging
import pickle

from indra_cogex.apps.constants import AGENT_NAME_CACHE
from indra_cogex.client import Neo4jClient
from indra_cogex.assembly import get_assembled_path

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Utility for search-related tasks.")
    parser.add_argument(
        "--force", action="store_true",
        help="Force re-generation agent cache.",
    )
    args = parser.parse_args()
    if not AGENT_NAME_CACHE.exists() or args.force:
        # Load straight from the tsv.gz file if it exists
        bioentity_path = get_assembled_path("BioEntity")
        if bioentity_path.exists():
            logger.info("Re-generating BioEntity cache from tsv.gz file")
            agent_cache = set()
            with gzip.open(bioentity_path, "rt") as f:
                reader = csv.reader(f, delimiter="\t")
                header = next(reader)
                for row in reader:
                    curie = row[0]
                    agent_cache.add(curie)
                    name = row[2]
                    if name:
                        agent_cache.add(name)
            if not agent_cache:
                raise RuntimeError("No agents found in BioEntity tsv.gz file")
        else:
            # Otherwise load from Neo4j
            logger.info("Re-generating BioEntity cache from Neo4j")
            neo4j_client = Neo4jClient()
            agent_cache = neo4j_client.load_agent_cache()

        with open(AGENT_NAME_CACHE, "wb") as f:
            pickle.dump(agent_cache, f)
