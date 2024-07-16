"""Metabolite-centric analysis blueprint."""

from typing import Dict, List, Mapping, Tuple
import pandas as pd
from indra.databases import chebi_client
from indra_cogex.client.enrichment.mla import (
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

def discrete_analysis(client, metabolites: str, method: str, alpha: float, keep_insignificant: bool,
                          minimum_evidence_count: int, minimum_belief: float):
    """Render the discrete metabolomic set analysis page."""
    metabolite_chebi_ids, errors = parse_metabolites_field(metabolites)

    results = metabolomics_ora(
        client=client,
        chebi_ids=metabolite_chebi_ids,
        method=method,
        alpha=alpha,
        keep_insignificant=keep_insignificant,
        minimum_evidence_count=minimum_evidence_count,
        minimum_belief=minimum_belief,
    )

    return {
        "metabolites": metabolite_chebi_ids,
        "errors": errors,
        "results": results
    }


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
