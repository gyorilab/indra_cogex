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
    """Parse a string of metabolite identifiers into ChEBI IDs and names.

    Args:
    s (str): A string containing metabolite identifiers (ChEBI IDs or CURIEs)

    Returns:
    Tuple[Dict[str, str], List[str]]: A tuple containing a dictionary of ChEBI IDs to metabolite names,
    and a list of any metabolite identifiers that couldn't be parsed."""
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
    """Perform discrete metabolite set analysis using metabolomics over-representation analysis.

    Args:
    client: The client object for making API calls
    metabolites (str): A string of metabolite identifiers
    method (str): The statistical method for multiple testing correction
    alpha (float): The significance level
    keep_insignificant (bool): Whether to keep statistically insignificant results
    minimum_evidence_count (int): Minimum number of evidence required for analysis
    minimum_belief (float): Minimum belief score for analysis

    Returns:
    dict: A dictionary containing results from the analysis"""
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


def enzyme_analysis(client, ec_code: str, chebi_ids: List[str] = None):
    """Perform enzyme analysis and explanation for given EC code and optional ChEBI IDs.

    Args:
    client: The client object for making API calls
    ec_code (str): The EC code for the enzyme
    chebi_ids (List[str], optional): List of ChEBI IDs for additional context

    Returns:
    List: A list of statements explaining the enzyme's function"""
    stmts = metabolomics_explanation(
        client=client, ec_code=ec_code, chebi_ids=chebi_ids
    )
    return stmts
