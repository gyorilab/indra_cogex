"""Proxies for the web application.

The point of this module is to make it possible to access global state
of the web application in all the various modules that have blueprints,
without creating circular imports.
"""

from flask import current_app
from werkzeug.local import LocalProxy

from ...client.neo4j_client import Neo4jClient

__all__ = [
    "INDRA_COGEX_EXTENSION",
    "client",
]

INDRA_COGEX_EXTENSION = "indra_cogex_client"

client: Neo4jClient = LocalProxy(lambda: current_app.extensions[INDRA_COGEX_EXTENSION])
