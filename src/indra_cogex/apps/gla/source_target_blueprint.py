"""Source-target gene relationship analysis blueprint."""

import flask
from flask_jwt_extended import jwt_required
from flask_wtf import FlaskForm
from wtforms import SubmitField, TextAreaField, StringField
from wtforms.validators import DataRequired

from indra_cogex.apps.proxies import client
from indra_cogex.analysis.source_targets_explanation import (
    run_explain_downstream_analysis,
    get_valid_gene_id,
    get_valid_gene_ids,
)
print("Loading source_target_blueprint.py")

__all__ = [
    "source_target_blueprint",
]

source_target_blueprint = flask.Blueprint("sta", __name__, url_prefix="/source_target")
print(f"Created blueprint with name: {source_target_blueprint.name}")


class SourceTargetForm(FlaskForm):
    """A form for source-target gene analysis."""
    source_gene = StringField(
        "Source Gene",
        description="Enter the source gene symbol (e.g., BRCA1)",
        validators=[DataRequired()],
    )

    target_genes = TextAreaField(
        "Target Genes",
        description="Enter target gene symbols, one per line",
        validators=[DataRequired()],
    )

    submit = SubmitField("Submit", render_kw={"id": "submit-btn"})


@source_target_blueprint.route("/analysis", methods=["GET", "POST"])
@jwt_required(optional=True)
def source_target_analysis_route():
    """Main analysis route."""
    form = SourceTargetForm()
    if form.validate_on_submit():
        # Get raw input from form
        source = form.source_gene.data.strip()
        targets = [g.strip() for g in form.target_genes.data.split('\n') if g.strip()]

        try:
            # Validate genes using analysis module functions
            source_id = get_valid_gene_id(source)
            if not source_id:
                flask.flash(f"Invalid source gene: {source}", "error")
                return flask.redirect(flask.url_for("sta.source_target_analysis_route"))

            target_ids = get_valid_gene_ids(targets)
            if not target_ids:
                flask.flash("No valid target genes provided", "error")
                return flask.redirect(flask.url_for("sta.source_target_analysis_route"))

            # Run analysis
            results = run_explain_downstream_analysis(
                source_id,
                target_ids,
                client=client
            )

            # Render results
            return flask.render_template(
                "source_target/source_target_results.html",
                source=source,
                targets=targets,
                source_id=source_id,
                target_genes={tid: get_valid_gene_id(t) for t, tid in zip(targets, target_ids)},
                results=results,
            )

        except Exception as e:
            flask.flash(str(e), "error")
            return flask.redirect(flask.url_for("sta.source_target_analysis_route"))

    return flask.render_template(
        "source_target/source_target_form.html",
        form=form,
    )

