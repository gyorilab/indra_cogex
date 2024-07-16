"""Metabolite-centric analysis blueprint."""

from typing import Dict, List, Mapping, Tuple

from indra_cogex.apps.proxies import client

from .fields import (
    alpha_field,
    correction_field,
    keep_insignificant_field,
    minimum_belief_field,
    minimum_evidence_field,
)
from ..utils import render_statements
from ...client.enrichment.mla import (
    EXAMPLE_CHEBI_CURIES,
    metabolomics_explanation,
    metabolomics_ora,
)



def parse_metabolites_field(s: str) -> Tuple[Dict[str, str], List[str]]:
    """Parse a metabolites field string."""
    records = {
        record.strip().strip('"').strip("'").strip()
        for line in s.strip().lstrip("[").rstrip("]").split()
        if line
        for record in line.strip().split(",")
        if record.strip()
    }
    chebi_ids = []
    errors = []
    for entry in records:
        if entry.isnumeric():
            chebi_ids.append(entry)
        elif entry.lower().startswith("chebi:chebi:"):
            chebi_ids.append(entry.lower().replace("chebi:chebi:", "", 1))
        elif entry.lower().startswith("chebi:"):
            chebi_ids.append(entry.lower().replace("chebi:", "", 1))
        else:  # probably a name, do our best
            chebi_id = chebi_client.get_chebi_id_from_name(entry)
            if chebi_id:
                chebi_ids.append(chebi_id)
            else:
                errors.append(entry)
    metabolites = {
        chebi_id: chebi_client.get_chebi_name_from_id(chebi_id)
        for chebi_id in chebi_ids
    }
    return metabolites, errors


metabolites_field = TextAreaField(
    "Metabolites",
    description="Paste your list of CHEBI identifiers, or"
    ' CURIEs here or click here to use <a href="#" onClick="exampleMetabolites()">an'
    " example list of metabolites</a>.",
    validators=[DataRequired()],
)





@metabolite_blueprint.route("/discrete", methods=["GET", "POST"])
def discrete_analysis():
    """Render the discrete metabolomic set analysis page."""
    form = DiscreteForm()
    if form.validate_on_submit():
        method = form.correction.data
        alpha = form.alpha.data
        keep_insignificant = form.keep_insignificant.data
        metabolite_chebi_ids, errors = form.parse_metabolites()

        results = metabolomics_ora(
            client=client,
            chebi_ids=metabolite_chebi_ids,
            method=method,
            alpha=alpha,
            keep_insignificant=keep_insignificant,
            minimum_evidence_count=form.minimum_evidence.data,
            minimum_belief=form.minimum_belief.data,
        )

        return flask.render_template(
            "metabolite_analysis/discrete_results.html",
            metabolites=metabolite_chebi_ids,
            errors=errors,
            method=method,
            alpha=alpha,
            results=results,
        )

    return flask.render_template(
        "metabolite_analysis/discrete_form.html",
        form=form,
        example_chebi_curies=", ".join(EXAMPLE_CHEBI_CURIES),
    )



def enzyme(ec_code: str):
    """Render the enzyme page."""
    user, roles = resolve_auth(dict(request.args))

    chebi_ids = request.args.get("q").split(",") if "q" in request.args else None
    _, identifier = bioregistry.normalize_parsed_curie("eccode", ec_code)
    if identifier is None:
        return flask.abort(400, f"Invalid EC Code: {ec_code}")
    stmts = metabolomics_explanation(
        client=client, ec_code=identifier, chebi_ids=chebi_ids
    )
    return render_statements(
        stmts,
        title=f"Statements for EC:{identifier}",
    )
