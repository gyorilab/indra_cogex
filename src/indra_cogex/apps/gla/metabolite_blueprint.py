"""Metabolite-centric blueprint."""

from typing import Dict, List, Mapping, Tuple

import bioregistry
import flask
from flask import request
from flask_jwt_extended import jwt_required
from flask_wtf import FlaskForm
from wtforms import SubmitField, TextAreaField
from wtforms.validators import DataRequired

from indra_cogex.apps.proxies import client
from indra_cogex.analysis.metabolite_analysis import (
    metabolite_discrete_analysis,
    enzyme_analysis,
    parse_metabolites,
)
from indra_cogex.client.enrichment.mla import EXAMPLE_CHEBI_CURIES

from .fields import (
    alpha_field,
    correction_field,
    keep_insignificant_field,
    minimum_belief_field,
    minimum_evidence_field,
    parse_text_field,
)
from ..utils import render_statements

__all__ = [
    "metabolite_blueprint",
]

metabolite_blueprint = flask.Blueprint("mla", __name__, url_prefix="/metabolite")


def parse_metabolites_field(s: str) -> Tuple[Dict[str, str], List[str]]:
    """Parse a metabolites field string.

    Parameters
    ----------
    s : str
        A string containing metabolite identifiers.

    Returns
    -------
    Tuple[Dict[str, str], List[str]]
        A tuple containing a dictionary of ChEBI IDs to metabolite names,
        and a list of any metabolite identifiers that couldn't be parsed."""
    records = parse_text_field(s)
    return parse_metabolites(records)


metabolites_field = TextAreaField(
    "Metabolites",
    description="Paste your list of CHEBI identifiers, or"
                ' CURIEs here or click here to use <a href="#" onClick="exampleMetabolites()">an'
                " example list of metabolites</a>.",
    validators=[DataRequired()],
)


class DiscreteForm(FlaskForm):
    """A form for discrete metabolute set enrichment analysis."""

    metabolites = metabolites_field
    minimum_evidence = minimum_evidence_field
    minimum_belief = minimum_belief_field
    alpha = alpha_field
    correction = correction_field
    keep_insignificant = keep_insignificant_field
    submit = SubmitField("Submit", render_kw={"id": "submit-btn"})

    def parse_metabolites(self) -> Tuple[Mapping[str, str], List[str]]:
        """Resolve the contents of the text field."""
        return parse_metabolites_field(self.metabolites.data)


class DiscreteForm(FlaskForm):
    """A form for discrete metabolite set enrichment analysis."""

    metabolites = metabolites_field
    minimum_evidence = minimum_evidence_field
    minimum_belief = minimum_belief_field
    alpha = alpha_field
    correction = correction_field
    keep_insignificant = keep_insignificant_field
    submit = SubmitField("Submit")

    def parse_metabolites(self) -> Tuple[Mapping[str, str], List[str]]:
        """Resolve the contents of the text field."""
        return parse_metabolites_field(self.metabolites.data)


@metabolite_blueprint.route("/discrete", methods=["GET", "POST"])
def discrete_analysis_route():
    """Render the discrete metabolomic set analysis page."""
    form = DiscreteForm()
    if form.validate_on_submit():
        metabolite_chebi_ids, errors = form.parse_metabolites()
        results = metabolite_discrete_analysis(
            client=client,
            metabolites=metabolite_chebi_ids,
            method=form.correction.data,
            alpha=form.alpha.data,
            keep_insignificant=form.keep_insignificant.data,
            minimum_evidence_count=form.minimum_evidence.data,
            minimum_belief=form.minimum_belief.data,
        )

        return flask.render_template(
            "metabolite_analysis/discrete_results.html",
            metabolites=metabolite_chebi_ids,
            errors=errors,
            method=form.correction.data,
            alpha=form.alpha.data,
            results=results,
        )

    return flask.render_template(
        "metabolite_analysis/discrete_form.html",
        form=form,
        example_chebi_curies=", ".join(EXAMPLE_CHEBI_CURIES),
    )


@metabolite_blueprint.route("/enzyme/<ec_code>", methods=["GET"])
@jwt_required(optional=True)
def enzyme_route(ec_code: str):
    """Render the enzyme page."""
    # Note: jwt_required is needed here because we're rendering a statement page

    chebi_ids = request.args.get("q").split(",") if "q" in request.args else None
    _, identifier = bioregistry.normalize_parsed_curie("eccode", ec_code)
    if identifier is None:
        return flask.abort(400, f"Invalid EC Code: {ec_code}")

    stmts = enzyme_analysis(client=client, ec_code=identifier, chebi_ids=chebi_ids)

    return render_statements(
        stmts,
        title=f"Statements for EC:{identifier}",
    )
