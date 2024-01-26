"""Curation app for INDRA CoGEx."""

import logging
import time

import flask
from flask import render_template


from .proxies import client

__all__ = [
    "entity_blueprint",
]

logger = logging.getLogger(__name__)
entity_blueprint = flask.Blueprint("entity", __name__, url_prefix="/explore")


@entity_blueprint.route("/entity/<prefix>:<identifier>", methods=["GET"])
def entity(prefix: str, identifier: str):
    """Get all statements about the given entity."""
    curie = f"{prefix}:{identifier}"
    print(f"querying for {curie}")
    start= time.time()
    node = client.get_node_by_curie(curie)
    print(f"done querying for {curie} in {time.time() - start:.2f} seconds")
    results = {}
    for rel in ["indra_rel", "associated_with", "mutated_in"]:
        query = f"""\
            MATCH (n:BioEntity {{id: '{curie}'}})-[r:{rel}]-(v)
            RETURN r, v
            LIMIT 5
        """
        start = time.time()
        results[rel] = list(client.query_tx(query))
        end = time.time() - start
        print(f"finished query for {rel} in {end} seconds.")
    return render_template("entity.html", node=node, results=results)
