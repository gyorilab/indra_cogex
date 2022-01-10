# -*- coding: utf-8 -*-

"""An app for gene list analysis."""

import os
from typing import List, Mapping, Tuple

import flask
from flask import flash, redirect, url_for
from flask_bootstrap import Bootstrap
from flask_wtf import FlaskForm
from indra.databases import hgnc_client
from more_click import make_web_command
from wtforms import BooleanField, FloatField, RadioField, SubmitField, TextAreaField

from indra_cogex.client.enrichment.gene_list import (
    EXAMPLE_GENE_IDS,
    go_ora,
    indra_downstream_ora,
    indra_upstream_ora,
    reactome_ora,
    wikipathways_ora,
)
from indra_cogex.client.enrichment.signed_gene_list import (
    EXAMPLE_DOWN_HGNC_IDS,
    EXAMPLE_UP_HGNC_IDS,
    reverse_causal_reasoning,
)
from indra_cogex.client.neo4j_client import Neo4jClient

app = flask.Flask(__name__)

# Secret key must be set to use flask-wtf, but there's no *really*
# secure information in this app so it's okay to set randomly
app.config["WTF_CSRF_ENABLED"] = False
app.config["SECRET_KEY"] = os.urandom(8)

bootstrap = Bootstrap()
bootstrap.init_app(app)

client = Neo4jClient()

genes_field = TextAreaField(
    "Genes",
    description="Paste your list of gene symbols, HGNC gene identifiers, or"
    ' CURIEs here or click here to use <a href="#" onClick="exampleGenes()">an'
    " example list of human genes</a> related to COVID-19.",
)
positive_genes_field = TextAreaField(
    "Positive Genes",
    description="Paste your list of gene symbols, HGNC gene identifiers, or CURIEs here",
)
negative_genes_field = TextAreaField(
    "Negative Genes",
    description="Paste your list of gene symbols, HGNC gene identifiers, or"
    ' CURIEs here or click here to use <a href="#" onClick="exampleGenes()">an'
    " example list</a> related to prostate cancer.",
)
indra_path_analysis_field = BooleanField("Include INDRA path-based analysis (slow)")
alpha_field = FloatField(
    "Alpha",
    default=0.05,
    description="The alpha is the threshold for significance in the"
    " Fisher's exact test with which multiple hypothesis"
    " testing correction will be executed.",
)
correction_field = RadioField(
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


def parse_genes_field(s: str) -> tuple[dict[str, str], list[str]]:
    """Parse a genes field string."""
    records = {
        record.strip().strip('"').strip("'").strip()
        for line in s.strip().lstrip("[").rstrip("]").split()
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


class DiscreteForm(FlaskForm):
    """A form for discrete gene set enrichment analysis."""

    genes = genes_field
    indra_path_analysis = indra_path_analysis_field
    alpha = alpha_field
    correction = correction_field
    submit = SubmitField("Submit")

    def parse_genes(self) -> Tuple[Mapping[str, str], List[str]]:
        """Resolve the contents of the text field."""
        return parse_genes_field(self.genes.data)


class SignedForm(FlaskForm):
    """A form for signed gene set enrichment analysis."""

    positive_genes = positive_genes_field
    negative_genes = negative_genes_field
    # alpha = alpha_field
    # correction = correction_field
    submit = SubmitField("Submit")

    def parse_positive_genes(self) -> Tuple[Mapping[str, str], List[str]]:
        """Resolve the contents of the text field."""
        return parse_genes_field(self.positive_genes.data)

    def parse_negative_genes(self) -> Tuple[Mapping[str, str], List[str]]:
        """Resolve the contents of the text field."""
        return parse_genes_field(self.negative_genes.data)


@app.route("/discrete", methods=["GET", "POST"])
def discretize_analysis():
    """Render the home page."""
    form = DiscreteForm()
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
        if form.indra_path_analysis.data:
            indra_upstream_results = indra_upstream_ora(
                client,
                gene_set,
                method=method,
                alpha=alpha,
            )
            indra_downstream_results = indra_downstream_ora(
                client,
                gene_set,
                method=method,
                alpha=alpha,
            )
        else:
            indra_upstream_results = None
            indra_downstream_results = None

        return flask.render_template(
            "discrete_results.html",
            genes=genes,
            errors=errors,
            method=method,
            alpha=alpha,
            go_results=go_results,
            wikipathways_results=wikipathways_results,
            reactome_results=reactome_results,
            indra_downstream_results=indra_downstream_results,
            indra_upstream_results=indra_upstream_results,
        )

    return flask.render_template(
        "discrete_form.html",
        form=form,
        example_hgnc_ids=", ".join(EXAMPLE_GENE_IDS),
    )


@app.route("/signed", methods=["GET", "POST"])
def signed_analysis():
    """Render the signed gene set enrichment analysis form."""
    form = SignedForm()
    if form.validate_on_submit():
        #method = form.correction.data
        #alpha = form.alpha.data
        positive_genes, positive_errors = form.parse_positive_genes()
        negative_genes, negative_errors = form.parse_negative_genes()
        results = reverse_causal_reasoning(
            client=client, up=positive_genes, down=negative_genes
        )
        return flask.render_template(
            "signed_results.html",
            positive_genes=positive_genes,
            positive_errors=positive_errors,
            negative_genes=negative_genes,
            negative_errors=negative_errors,
            results=results,
            #method=method,
            #alpha=alpha,
        )
    return flask.render_template(
        "signed_form.html",
        form=form,
        example_positive_hgnc_ids=", ".join(EXAMPLE_UP_HGNC_IDS),
        example_negative_hgnc_ids=", ".join(EXAMPLE_DOWN_HGNC_IDS),
    )


@app.route("/")
def home():
    """Render the home page."""
    return redirect(url_for(discretize_analysis.__name__))


@app.route("/continuous", methods=["GET", "POST"])
def continuous_analysis():
    """Render the scored analysis form."""
    flash("Scored analysis endpoint has not yet been implemented")
    return redirect(url_for(home.__name__))


cli = make_web_command(app=app)

if __name__ == "__main__":
    cli()
