# -*- coding: utf-8 -*-

"""An app for gene list analysis."""

import os
from collections import Counter
from functools import lru_cache
from typing import Dict, List, Mapping, Tuple

import flask
import pandas as pd
from flask_bootstrap import Bootstrap4
from flask_wtf import FlaskForm
from indra.databases import hgnc_client
from more_click import make_web_command
from wtforms import (
    BooleanField,
    FileField,
    FloatField,
    IntegerField,
    RadioField,
    SubmitField,
    TextAreaField,
)
from wtforms.validators import DataRequired

from indra_cogex.client.enrichment.continuous import get_rat_scores, go_gsea
from indra_cogex.client.enrichment.discrete import (
    EXAMPLE_GENE_IDS,
    go_ora,
    indra_downstream_ora,
    indra_upstream_ora,
    reactome_ora,
    wikipathways_ora,
)
from indra_cogex.client.enrichment.signed import (
    EXAMPLE_NEGATIVE_HGNC_IDS,
    EXAMPLE_POSITIVE_HGNC_IDS,
    reverse_causal_reasoning,
)
from indra_cogex.client.neo4j_client import Neo4jClient
from indra_cogex.client.queries import get_edge_counter, get_node_counter

app = flask.Flask(__name__)

# Secret key must be set to use flask-wtf, but there's no *really*
# secure information in this app so it's okay to set randomly
app.config["WTF_CSRF_ENABLED"] = False
app.config["SECRET_KEY"] = os.urandom(8)

bootstrap = Bootstrap4(app)

client = Neo4jClient()

genes_field = TextAreaField(
    "Genes",
    description="Paste your list of gene symbols, HGNC gene identifiers, or"
                ' CURIEs here or click here to use <a href="#" onClick="exampleGenes()">an'
                " example list of human genes</a> related to COVID-19.",
    validators=[DataRequired()],
)
positive_genes_field = TextAreaField(
    "Positive Genes",
    description="Paste your list of gene symbols, HGNC gene identifiers, or CURIEs here",
    validators=[DataRequired()],
)
negative_genes_field = TextAreaField(
    "Negative Genes",
    description="Paste your list of gene symbols, HGNC gene identifiers, or"
                ' CURIEs here or click here to use <a href="#" onClick="exampleGenes()">an'
                " example list</a> related to prostate cancer.",
    validators=[DataRequired()],
)
indra_path_analysis_field = BooleanField("Include INDRA path-based analysis (slow)")
alpha_field = FloatField(
    "Alpha",
    default=0.05,
    validators=[DataRequired()],
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

file_field = FileField("File", validators=[DataRequired()])
species_field = RadioField(
    "Species",
    choices=[
        ("human", "Human"),
        ("rat", "Rat"),
        ("mouse", "Mouse"),
    ],
    default="human",
)
permutations_field = IntegerField(
    "Permutations",
    default=100,
    validators=[DataRequired()],
    description="The number of permutations used with GSEA",
)


def parse_genes_field(s: str) -> Tuple[Dict[str, str], List[str]]:
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


class ContinuousForm(FlaskForm):
    """A form for continuous gene set enrichment analysis."""

    file = file_field
    species = species_field
    permutations = permutations_field
    submit = SubmitField("Submit")

    def get_scores(self) -> Dict[str, float]:
        """Get scores dictionary."""
        name = self.file.data.filename
        sep = "," if name.endswith("csv") else "\t"
        df = pd.read_csv(self.file.data, sep=sep)
        if self.species.data == "rat":
            scores = get_rat_scores(df)
        elif self.species.data == "mouse":
            raise NotImplementedError
        elif self.species.data == "human":
            raise NotImplementedError
        else:
            raise ValueError
        return scores


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
        # method = form.correction.data
        # alpha = form.alpha.data
        positive_genes, positive_errors = form.parse_positive_genes()
        negative_genes, negative_errors = form.parse_negative_genes()
        results = reverse_causal_reasoning(
            client=client,
            positive_hgnc_ids=positive_genes,
            negative_hgnc_ids=negative_genes,
        )
        return flask.render_template(
            "signed_results.html",
            positive_genes=positive_genes,
            positive_errors=positive_errors,
            negative_genes=negative_genes,
            negative_errors=negative_errors,
            results=results,
            # method=method,
            # alpha=alpha,
        )
    return flask.render_template(
        "signed_form.html",
        form=form,
        example_positive_hgnc_ids=", ".join(EXAMPLE_POSITIVE_HGNC_IDS),
        example_negative_hgnc_ids=", ".join(EXAMPLE_NEGATIVE_HGNC_IDS),
    )


@app.route("/continuous", methods=["GET", "POST"])
def continuous_analysis():
    """Render the continuous analysis form."""
    form = ContinuousForm()
    if form.validate_on_submit():
        scores = form.get_scores()
        go_results = go_gsea(
            client=client, scores=scores, permutation_num=form.permutations.data
        )
        return flask.render_template(
            "continuous_results.html",
            go_results=go_results,
        )
    return flask.render_template(
        "continuous_form.html",
        form=form,
    )


@lru_cache(1)
def _get_counters() -> Tuple[Counter, Counter]:
    prod = False
    if prod:
        node_counter = get_node_counter(client)
        edge_counter = get_edge_counter(client)
    else:
        node_counter = Counter(
            {
                "ClinicalTrial": 364_937,
                "Evidence": 18_326_675,
                "BioEntity": 2_612_711,
                "Publication": 33_369_469,
            }
        )
        edge_counter = Counter(
            {
                "expressed_in": 4_725_039,
                "copy_number_altered_in": 1_422_111,
                "sensitive_to": 69_271,
                "mutated_in": 1_140_475,
                "has_indication": 45_902,
                "tested_in": 253_578,
                "has_trial": 536_104,
                "indra_rel": 6_268_226,
                "has_citation": 17_975_205,
                "associated_with": 158_648,
                "isa": 534_776,
                "xref": 1_129_208,
                "partof": 619_379,
                "annotated_with": 290_131_450,
                "haspart": 428_980,
                "has_side_effect": 308_948,
            }
        )
    return node_counter, edge_counter


def _figure_number(n: int) -> str:
    if n > 1_000_000:
        lead = n / 1_000_000
        if lead < 10:
            return f"{round(lead, 1)}M"
        else:
            return f"{round(lead)}M"
    if n > 1_000:
        lead = n / 1_000
        if lead < 10:
            return f"{round(lead, 1)}K"
        else:
            return f"{round(lead)}K"
    else:
        return str(n)


@app.route("/")
def home():
    """Render the home page."""
    node_counter, edge_counter = _get_counters()
    return flask.render_template(
        "home.html",
        format_number=_figure_number,
        node_counter=node_counter,
        edge_counter=edge_counter,
    )


cli = make_web_command(app=app)

if __name__ == "__main__":
    cli()
