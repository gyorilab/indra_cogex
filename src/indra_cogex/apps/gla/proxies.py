from flask import current_app
from werkzeug.local import LocalProxy

from indra_cogex.client.neo4j_client import Neo4jClient

__all__ = [
    "INDRA_COGEX_EXTENSION",
    "client",
]

INDRA_COGEX_EXTENSION = "indra_cogex_client"


def _get_client() -> Neo4jClient:
    return current_app.extensions[INDRA_COGEX_EXTENSION]


client = LocalProxy(_get_client)
