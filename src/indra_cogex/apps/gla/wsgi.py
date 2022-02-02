# -*- coding: utf-8 -*-

"""An app for gene list analysis."""

import os
from collections import Counter
from functools import lru_cache
from typing import Tuple

import flask
from flask_bootstrap import Bootstrap4
from more_click import make_web_command

from .gene_blueprint import gene_blueprint
from .metabolite_blueprint import metabolite_blueprint
from .proxies import INDRA_COGEX_EXTENSION, client
from ...client.neo4j_client import Neo4jClient
from ...client.queries import get_edge_counter, get_node_counter

app = flask.Flask(__name__)

# Secret key must be set to use flask-wtf, but there's no *really*
# secure information in this app so it's okay to set randomly
app.config["WTF_CSRF_ENABLED"] = False
app.config["SECRET_KEY"] = os.urandom(8)

bootstrap = Bootstrap4(app)

if not hasattr(app, "extensions"):
    print("no extensions?")
    app.extensions = {}

app.extensions[INDRA_COGEX_EXTENSION] = Neo4jClient()


@lru_cache(1)
def _get_counters() -> Tuple[Counter, Counter]:
    node_counter = get_node_counter(client)
    edge_counter = get_edge_counter(client)
    return node_counter, edge_counter


def _figure_number(n: int):
    if n > 1_000_000:
        lead = n / 1_000_000
        if lead < 10:
            return round(lead, 1), "M"
        else:
            return round(lead), "M"
    if n > 1_000:
        lead = n / 1_000
        if lead < 10:
            return round(lead, 1), "K"
        else:
            return round(lead), "K"
    else:
        return n, ""


edge_labels = {
    "annotated_with": "MeSH Annotations",
    "associated_with": "GO Annotations",
    "has_citation": "Citations",
    "indra_rel": "Causal Relations",
    "expressed_in": "Gene Expressions",
    "copy_number_altered_in": "CNVs",
    "mutated_in": "Mutations",
    "xref": "Xrefs",
    "partof": "Part Of",
    "has_trial": "Disease Trials",
    "isa": "Subclasses",
    "haspart": "Has Part",
    "has_side_effect": "Side Effects",
    "tested_in": "Drug Trials",
    "sensitive_to": "Sensitivities",
    "has_indication": "Drug Indications",
}


@app.route("/")
def home():
    """Render the home page."""
    node_counter, edge_counter = _get_counters()
    return flask.render_template(
        "home.html",
        format_number=_figure_number,
        node_counter=node_counter,
        edge_counter=edge_counter,
        edge_labels=edge_labels,
    )


app.register_blueprint(gene_blueprint)
app.register_blueprint(metabolite_blueprint)

cli = make_web_command(app=app)

if __name__ == "__main__":
    cli()
