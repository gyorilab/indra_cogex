"""Gene-centric blueprint."""
from http import HTTPStatus
from pathlib import Path
from typing import List, Mapping, Tuple

import flask
import pandas as pd
from flask import url_for, abort
from flask_wtf import FlaskForm
from wtforms import BooleanField, SubmitField, TextAreaField, StringField
from wtforms.validators import DataRequired
import io


from indra_cogex.analysis.gene_analysis import (
    discrete_analysis,
    signed_analysis,
    continuous_analysis,
    parse_gene_list,
)
from indra_cogex.apps.constants import INDRA_COGEX_WEB_LOCAL
from indra_cogex.apps.proxies import client
from indra_cogex.client.enrichment.discrete import EXAMPLE_GENE_IDS
from indra_cogex.client.enrichment.signed import (
    EXAMPLE_NEGATIVE_HGNC_IDS,
    EXAMPLE_POSITIVE_HGNC_IDS
)
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
    species_field, parse_text_field,
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


class DiscreteForm(FlaskForm):
    """A form for discrete gene set enrichment analysis."""

    genes = genes_field
    background_genes = TextAreaField(
        "Background Genes (Optional)",
        description='Enter background genes. If not provided, all human genes will be used. ',
        render_kw={"rows": 2, "cols": 50}
    )
    indra_path_analysis = indra_path_analysis_field
    minimum_evidence = minimum_evidence_field
    minimum_belief = minimum_belief_field
    alpha = alpha_field
    correction = correction_field
    keep_insignificant = keep_insignificant_field
    if INDRA_COGEX_WEB_LOCAL:
        local_download = BooleanField("local_download")
    submit = SubmitField("Submit", render_kw={"id": "submit-btn"})

    def parse_genes(self) -> Tuple[Mapping[str, str], List[str]]:
        """Resolve the contents of the text field."""
        gene_set = parse_text_field(self.genes.data)
        return parse_gene_list(gene_set)

    def parse_background_genes(self) -> Tuple[Mapping[str, str], List[str]]:
        """Resolve the contents of the background genes field."""
        if not self.background_genes.data:
            return {}, []
        gene_set = parse_text_field(self.background_genes.data)
        return parse_gene_list(gene_set)


class SignedForm(FlaskForm):
    """A form for signed gene set enrichment analysis."""

    positive_genes = positive_genes_field
    negative_genes = negative_genes_field
    minimum_evidence = minimum_evidence_field
    minimum_belief = minimum_belief_field
    alpha = alpha_field
    keep_insignificant = keep_insignificant_field
    submit = SubmitField("Submit", render_kw={"id": "submit-btn"})

    def parse_positive_genes(self) -> Tuple[Mapping[str, str], List[str]]:
        """Resolve the contents of the text field."""
        gene_set = parse_text_field(self.positive_genes.data)
        return parse_gene_list(gene_set)

    def parse_negative_genes(self) -> Tuple[Mapping[str, str], List[str]]:
        """Resolve the contents of the text field."""
        gene_set = parse_text_field(self.negative_genes.data)
        return parse_gene_list(gene_set)


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
    submit = SubmitField("Submit", render_kw={"id": "submit-btn"})


@gene_blueprint.route("/discrete", methods=["GET", "POST"])
def discretize_analysis():
    """Render the discrete gene analysis page and handle form submission.

    Returns
    -------
    str
        Rendered HTML template."""

    form = DiscreteForm()
    if form.validate_on_submit():
        genes, errors = form.parse_genes()
        background_genes, background_errors = form.parse_background_genes()

        # Combine any parsing errors
        all_errors = errors + background_errors if background_errors else errors

        results = discrete_analysis(
            list(genes),
            client=client,
            method=form.correction.data,
            alpha=form.alpha.data,
            keep_insignificant=form.keep_insignificant.data,
            minimum_evidence_count=form.minimum_evidence.data,
            minimum_belief=form.minimum_belief.data,
            indra_path_analysis=form.indra_path_analysis.data,
            background_gene_list=list(background_genes) if background_genes else None
        )

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
            background_genes=background_genes,
            errors=all_errors,
            method=form.correction.data,
            alpha=form.alpha.data,
            minimum_evidence=form.minimum_evidence.data,
            minimum_belief=form.minimum_belief.data,
            go_results=results["go"],
            wikipathways_results=results["wikipathways"],
            reactome_results=results["reactome"],
            phenotype_results=results["phenotype"],
            indra_downstream_results=results.get("indra-downstream"),
            indra_upstream_results=results.get("indra-upstream"),
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
            list(positive_genes),
            list(negative_genes),
            client=client,
            alpha=form.alpha.data,
            keep_insignificant=form.keep_insignificant.data,
            minimum_evidence_count=form.minimum_evidence.data,
            minimum_belief=form.minimum_belief.data
        )

        return flask.render_template(
            "gene_analysis/signed_results.html",
            positive_genes=positive_genes,
            positive_errors=positive_errors,
            negative_genes=negative_genes,
            negative_errors=negative_errors,
            results=results,
            minimum_evidence=form.minimum_evidence.data,
            minimum_belief=form.minimum_belief.data
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

        # Get file path and read the data into a DataFrame
        file_name = form.file.data.filename
        file = form.file.data
        gene_name_column = form.gene_name_column.data
        log_fold_change_column = form.log_fold_change_column.data
        sep = "," if file_name.endswith(".csv") else "\t"

        try:
            file_data = file.read().decode("utf-8")
            df = pd.read_csv(io.StringIO(file_data), sep=sep)
        except Exception as e:
            abort(
                HTTPStatus.BAD_REQUEST,
                f"Error reading input file: {str(e)}"
            )

        if len(df) < 2:

            abort(
                HTTPStatus.BAD_REQUEST,
                "Input file contains insufficient data. At least 2 genes are required."
            )

        if not {gene_name_column, log_fold_change_column}.issubset(df.columns):
            abort(
                HTTPStatus.BAD_REQUEST,
                "Gene name and log fold change columns must be present in the input file."
            )

        results = continuous_analysis(
            gene_names=df[gene_name_column].values,
            log_fold_change=df[log_fold_change_column].values,
            species=form.species.data,
            permutations=form.permutations.data,
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
