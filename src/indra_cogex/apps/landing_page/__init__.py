import logging
from functools import lru_cache
from typing import Tuple, Counter

from flask import render_template
from more_click import make_web_command

from indra_cogex.apps.proxies import client
from .. import get_flask_app
from ...client.queries import get_edge_counter, get_node_counter

logger = logging.getLogger(__name__)

app = get_flask_app(__name__)

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


@lru_cache(1)
def _get_counters() -> Tuple[Counter, Counter]:
    node_counter = get_node_counter(client=client)
    edge_counter = get_edge_counter(client=client)
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


@app.route("/")
def home():
    """Render the home page."""
    node_counter, edge_counter = _get_counters()
    return render_template(
        "home.html",
        format_number=_figure_number,
        node_counter=node_counter,
        edge_counter=edge_counter,
        edge_labels=edge_labels,
    )


# Create runnable cli command
cli = make_web_command(app)

if __name__ == "__main__":
    cli()
