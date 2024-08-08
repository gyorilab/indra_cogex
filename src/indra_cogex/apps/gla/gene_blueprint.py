"""Gene-centric blueprint."""

from pathlib import Path
from typing import Dict, List, Mapping, Tuple

import flask
import pandas as pd
from flask import url_for
from flask_wtf import FlaskForm
from indra.databases import hgnc_client
from wtforms import BooleanField, SubmitField, TextAreaField, StringField
from wtforms.validators import DataRequired

from indra_cogex.apps.constants import INDRA_COGEX_WEB_LOCAL
from indra_cogex.apps.proxies import client
from .fields import (
    alpha_field,
    correction_field,
    file_field,
    indra_path_analysis_field,
    keep_insignificant_field,
    minimum_belief_field,
    minimum_evidence_field,
    permutations_field,
    source_field,
    species_field,
)

from indra_cogex.analysis.gene_analysis import (
    discrete_analysis,
    signed_analysis,
    continuous_analysis
)

__all__ = ["gene_blueprint"]

gene_blueprint = flask.Blueprint("gla", __name__, url_prefix="/gene")

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


def parse_genes_field(s: str) -> Tuple[Dict[str, str], List[str]]:
    """Parse a gene field string."""
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
            hgnc_ids.append(entry.lower().replace("hgnc:", "", 1))
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
    minimum_evidence = minimum_evidence_field
    minimum_belief = minimum_belief_field
    alpha = alpha_field
    correction = correction_field
    keep_insignificant = keep_insignificant_field
    if INDRA_COGEX_WEB_LOCAL:
        local_download = BooleanField("local_download")
    submit = SubmitField("Submit")

    def parse_genes(self) -> Tuple[Mapping[str, str], List[str]]:
        """Resolve the contents of the text field."""
        return parse_genes_field(self.genes.data)


class SignedForm(FlaskForm):
    """A form for signed gene set enrichment analysis."""

    positive_genes = positive_genes_field
    negative_genes = negative_genes_field
    minimum_evidence = minimum_evidence_field
    minimum_belief = minimum_belief_field
    alpha = alpha_field
    # correction = correction_field
    keep_insignificant = keep_insignificant_field
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
    gene_name_column = StringField(
        "Gene Name Column",
        description="The name of the column containing gene names (HGNC symbols) in the "
                    "uploaded file.",
        validators=[DataRequired()],
    )
    log_fold_change_column = StringField(
        "Ranking Metric Column",
        description="The name of the column containing the ranking metric values in the "
                    "uploaded file.",
        validators=[DataRequired()],
    )
    species = species_field
    permutations = permutations_field
    alpha = alpha_field
    keep_insignificant = keep_insignificant_field
    source = source_field
    minimum_evidence = minimum_evidence_field
    minimum_belief = minimum_belief_field
    submit = SubmitField("Submit")


@gene_blueprint.route("/discrete", methods=["GET", "POST"])
def discretize_analysis():
    """Render the discrete analysis page and handle form submission.

    Returns
    -------
    str
        Rendered HTML template."""
    form = DiscreteForm()
    if form.validate_on_submit():
        genes, errors = form.parse_genes()
        results = discrete_analysis(
            genes,
            client=client,
            method=form.correction.data,
            alpha=form.alpha.data,
            keep_insignificant=form.keep_insignificant.data,
            minimum_evidence_count=form.minimum_evidence.data,
            minimum_belief=form.minimum_belief.data,
            indra_path_analysis=form.indra_path_analysis.data  # Include this line
        )
        results['parsing_errors'] = errors

        if INDRA_COGEX_WEB_LOCAL and form.local_download.data:
            downloads = Path.home().joinpath("Downloads")
            for key, df in results.items():
                if isinstance(df, pd.DataFrame):
                    df.to_csv(downloads.joinpath(f"{key}.tsv"), sep="\t", index=False)
            flask.flash(f"Downloaded files to {downloads}")
            return flask.redirect(url_for(f".{discretize_analysis.__name__}"))

        return flask.render_template(
            "gene_analysis/discrete_results.html",
            genes=genes,
            **results
        )

    return flask.render_template(
        "gene_analysis/discrete_form.html",
        form=form,
        example_hgnc_ids=", ".join(EXAMPLE_GENE_IDS),
    )


@gene_blueprint.route("/signed", methods=["GET", "POST"])
def signed_analysis_route():
    """Render the signed gene set enrichment analysis form and handle form submission.

    Returns
    -------
    str
        Rendered HTML template."""
    form = SignedForm()
    if form.validate_on_submit():
        positive_genes, positive_errors = form.parse_positive_genes()
        negative_genes, negative_errors = form.parse_negative_genes()
        results = signed_analysis(
            positive_genes,
            negative_genes,
            client=client,
            alpha=form.alpha.data,
            keep_insignificant=form.keep_insignificant.data,
            minimum_evidence_count=form.minimum_evidence.data,
            minimum_belief=form.minimum_belief.data
        )
        results['positive_parsing_errors'] = positive_errors
        results['negative_parsing_errors'] = negative_errors

        return flask.render_template(
            "gene_analysis/signed_results.html",
            positive_genes=positive_genes,
            negative_genes=negative_genes,
            **results
        )
    return flask.render_template(
        "gene_analysis/signed_form.html",
        form=form,
        example_positive_hgnc_ids=", ".join(EXAMPLE_POSITIVE_HGNC_IDS),
        example_negative_hgnc_ids=", ".join(EXAMPLE_NEGATIVE_HGNC_IDS),
    )


@gene_blueprint.route("/continuous", methods=["GET", "POST"])
def continuous_analysis_route():
    """Render the continuous analysis form and handle form submission.

    Returns
    -------
    str
        Rendered HTML template."""
    form = ContinuousForm()
    if form.validate_on_submit():
        file_path = form.file.data.filename
        results = continuous_analysis(
            file_path,
            form.gene_name_column.data,
            form.log_fold_change_column.data,
            form.species.data,
            form.permutations.data,
            client=client,
            alpha=form.alpha.data,
            keep_insignificant=form.keep_insignificant.data,
            source=form.source.data,
            minimum_evidence_count=form.minimum_evidence.data,
            minimum_belief=form.minimum_belief.data
        )

        return flask.render_template(
            "gene_analysis/continuous_results.html",
            source=form.source.data,
            results=results,
        )
    return flask.render_template(
        "gene_analysis/continuous_form.html",
        form=form,
    )
