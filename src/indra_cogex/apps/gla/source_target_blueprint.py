"""Source-target gene relationship analysis blueprint."""

import flask
from flask_jwt_extended import jwt_required
from flask_wtf import FlaskForm
from wtforms import SubmitField, TextAreaField, StringField
from wtforms.validators import DataRequired

from indra_cogex.apps.constants import VUE_SRC_JS, VUE_SRC_CSS, sources_dict
from indra_cogex.apps.proxies import client
from indra_cogex.analysis.source_targets_explanation import (
    run_explain_downstream_analysis,
    get_valid_gene_id,
    get_valid_gene_ids,
)

__all__ = [
    "source_target_blueprint",
]

source_target_blueprint = flask.Blueprint("sta", __name__, url_prefix="/source_target")


class SourceTargetForm(FlaskForm):
    """A form for source-target gene analysis."""
    source_gene = StringField(
        "Source Gene",
        description="Enter the source gene symbol (e.g., BRCA1)",
        validators=[DataRequired()],
    )

    target_genes = TextAreaField(
        "Target Genes",
        description="Enter target gene symbols, separated by commas or new lines" 
                    " or, <a href=\"#\" onClick=\"fillExampleTargetGenes()\">click here for examples</a>",
        validators=[DataRequired()],
    )

    submit = SubmitField("Submit", render_kw={"id": "submit-btn"})


def process_target_genes(target_genes_input):
    """Process target genes from the form input.

    Handles both comma-separated and newline-separated formats.

    Parameters
    ----------
    target_genes_input : str
        Raw input from the form

    Returns
    -------
    list
        List of cleaned gene symbols
    """
    # First check if there are commas in the input
    if ',' in target_genes_input:
        # Split by commas
        genes = target_genes_input.split(',')
    else:
        # Split by newlines
        genes = target_genes_input.splitlines()

    # Clean up whitespace and empty entries
    genes = [gene.strip() for gene in genes if gene.strip()]

    return genes


@source_target_blueprint.route("/analysis", methods=["GET", "POST"])
@jwt_required(optional=True)
def source_target_analysis_route():
    """Main analysis route."""
    form = SourceTargetForm()

    # Define example genes for the template
    example_target_genes = "TP53, PARP1, RAD51, CHEK2"

    if form.validate_on_submit():
        source = form.source_gene.data.strip()
        targets = process_target_genes(form.target_genes.data)

        try:
            source_id = get_valid_gene_id(source)
            target_ids = get_valid_gene_ids(targets)

            if not source_id:
                flask.flash(f"Invalid source gene: {source}")
                return flask.redirect(flask.url_for("sta.source_target_analysis_route"))

            if not target_ids:
                flask.flash("No valid target genes provided")
                return flask.redirect(flask.url_for("sta.source_target_analysis_route"))

            results = run_explain_downstream_analysis(
                source_id,
                target_ids,
                client=client
            )

            return flask.render_template(
                "source_target/source_target_results.html",
                source=source,
                targets=targets,
                source_id=source_id,
                target_genes={tid: get_valid_gene_id(t) for t, tid in zip(targets, target_ids)},
                results=results,
                sources_dict=sources_dict,
                vue_src_js=VUE_SRC_JS,
                vue_src_css=VUE_SRC_CSS,
            )

        except Exception as e:
            # todo: one exception is raised which gives this error:
            # "the JSON object must be str, bytes or bytearray, not dict"
            flask.flash("An error occurred: " + str(e), "error")
            return flask.redirect(flask.url_for("sta.source_target_analysis_route"))

    return flask.render_template(
        "source_target/source_target_form.html",
        form=form,
        example_target_genes=example_target_genes,
    )
