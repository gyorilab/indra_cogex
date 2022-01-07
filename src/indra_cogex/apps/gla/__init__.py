# -*- coding: utf-8 -*-

"""An app for gene list analysis."""

import os
from typing import List, Mapping, Tuple

import flask
from flask_bootstrap import Bootstrap
from flask_wtf import FlaskForm
from more_click import make_web_command
from wtforms import FloatField, RadioField, SubmitField, TextAreaField

from indra.databases import hgnc_client
from indra_cogex.client.gene_list import go_ora, reactome_ora, wikipathways_ora
from indra_cogex.client.neo4j_client import Neo4jClient

app = flask.Flask(__name__)

# Secret key must be set to use flask-wtf, but there's no *really*
# secure information in this app so it's okay to set randomly
app.config["WTF_CSRF_ENABLED"] = False
app.config["SECRET_KEY"] = os.urandom(8)

bootstrap = Bootstrap()
bootstrap.init_app(app)

client = Neo4jClient()


class GeneForm(FlaskForm):
    """A form for gene lists"""

    genes = TextAreaField(
        "Genes",
        description="Paste your list of gene symbol, HGNC gene identifiers, or"
        " CURIEs here.",
    )
    alpha = FloatField(
        "Alpha",
        default=0.05,
        description="The alpha is the threshold for significance in the"
        " Fisher's exact test with which multiple hypothesis"
        " testing correction will be executed.",
    )
    correction = RadioField(
        "Multiple Hypothesis Test Correction",
        choices=[
            ("fdr_bh", "Family-wise Correction with Benjamini/Hochberg"),
            ("bonferroni", "Bonferroni (one-step correction)"),
            ("sidak", "Sidak (one-step correction)"),
            ("holm-sidak", "Holm-Sidak (step down method using Sidak adjustments)"),
            ("holm", "Holm (step-down method using Bonferroni adjustments)"),
            ("fdr_tsbh", "Two step Benjamini and Hochberg procedure"),
            (
                "fdr_tsbky",
                "Two step estimation method of Benjamini, Krieger, and Yekutieli",
            ),
        ],
        default="fdr_bh",
    )
    submit = SubmitField("Submit")

    def parse_genes(self) -> Tuple[Mapping[str, str], List[str]]:
        """Resolve the contents of the text field."""
        records = {
            record.strip().strip('"').strip("'").strip()
            for line in self.genes.data.strip().lstrip("[").rstrip("]").split()
            if line
            for record in line.strip().split(",")
            if record.strip()
        }
        hgnc_ids = []
        errors = []
        for entry in records:
            if entry.lower().startswith("hgnc:"):
                hgnc_ids.append(entry.lower().removeprefix("hgnc:"))
            elif entry.isnumeric():
                hgnc_ids.append(entry)
            else:  # probably a symbol
                hgnc_id = hgnc_client.get_current_hgnc_id(entry)
                if hgnc_id:
                    hgnc_ids.append(hgnc_id)
                else:
                    errors.append(entry)
        genes = {hgnc_id: hgnc_client.get_hgnc_name(hgnc_id) for hgnc_id in hgnc_ids}
        return genes, errors


@app.route("/", methods=["GET", "POST"])
def home():
    """Render the home page."""
    form = GeneForm()
    if form.validate_on_submit():
        method = form.correction.data
        alpha = form.alpha.data
        genes, errors = form.parse_genes()
        gene_set = set(genes)

        go_results = go_ora(
            client,
            gene_set,
            method=method,
            alpha=alpha,
        )
        wikipathways_results = wikipathways_ora(
            client,
            gene_set,
            method=method,
            alpha=alpha,
        )
        reactome_results = reactome_ora(
            client,
            gene_set,
            method=method,
            alpha=alpha,
        )

        return flask.render_template(
            "results.html",
            genes=genes,
            errors=errors,
            method=method,
            alpha=alpha,
            go_results=go_results,
            wikipathways_results=wikipathways_results,
            reactome_results=reactome_results,
        )

    return flask.render_template(
        "home.html",
        form=GeneForm(),
    )


cli = make_web_command(app=app)

if __name__ == "__main__":
    cli()
