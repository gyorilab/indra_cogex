"""Proxies for the web application.

The point of this module is to make it possible to access global state
of the web application in all the various modules that have blueprints,
without creating circular imports.
"""

from flask import current_app, request
from werkzeug.local import LocalProxy

from indra_cogex.apps.constants import INDRA_COGEX_EXTENSION, STATEMENT_CURATION_CACHE
from indra_cogex.apps.curator import CurationCache
from indra_cogex.client.neo4j_client import Neo4jClient

__all__ = [
    "client",
    "curation_cache",
    "limit",
    "filter_curated",
]

client: Neo4jClient = LocalProxy(lambda: current_app.extensions[INDRA_COGEX_EXTENSION])

curation_cache: CurationCache = LocalProxy(
    lambda: current_app.extensions[STATEMENT_CURATION_CACHE]
)

limit: int = LocalProxy(lambda: request.args.get("limit", type=int, default=25))

filter_curated: bool = LocalProxy(
    lambda: request.args.get("filter_curated", default="true").lower() in {"true", "t"}
)
