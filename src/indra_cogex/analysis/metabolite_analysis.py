"""Metabolite-centric analysis."""

from typing import Dict, List, Tuple, Iterable
import logging
import pandas as pd

from indra.databases import chebi_client
from indra_cogex.client.enrichment.mla import (
    metabolomics_explanation,
    metabolomics_ora,
)
from indra_cogex.client.neo4j_client import Neo4jClient, autoclient

logger = logging.getLogger(__name__)


@autoclient()
def metabolite_discrete_analysis(
        metabolites: List[str],
        method: str = "fdr_bh",
        alpha: float = 0.05,
        keep_insignificant: bool = False,
        minimum_evidence_count: int = 1,
        minimum_belief: float = 0.0,
        *,
        client: Neo4jClient  # Client argument moved to the end as a keyword argument
) -> pd.DataFrame:
    """Perform discrete metabolite analysis and return results as a DataFrame

    Parameters
    ----------
    metabolites : List[str]
        List of metabolite identifiers (CHEBI IDs or CHEBI names).
    method : str, optional
        Method to adjust p-values, default is family-wise correction with
        Benjamini/Hochberg.
    alpha : float, optional
        Significance level, default is 0.05.
    keep_insignificant : bool, optional
        Whether to retain insignificant results, default is False.
    minimum_evidence_count : int, optional
        Minimum evidence count threshold, default is 1.
    minimum_belief : float, optional
        Minimum belief threshold for filtering results, default is 0.
    client : Neo4jClient, optional
        Neo4j client for database interaction, injected via autoclient.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the analysis results.
    """
    chebi_ids, errors = parse_metabolites(metabolites)
    if errors:
        logger.warning(f"Could not parse the following metabolites: {errors}")

    # Perform the metabolomics ORA analysis
    ora_results = metabolomics_ora(
        client=client,
        chebi_ids=chebi_ids,
        method=method,
        alpha=alpha,
        keep_insignificant=keep_insignificant,
        minimum_evidence_count=minimum_evidence_count,
        minimum_belief=minimum_belief,
    )

    return ora_results


@autoclient()
def enzyme_analysis(
        ec_code: str,
        chebi_ids: List[str] = None,
        *,
        client: Neo4jClient
):
    """Perform enzyme analysis for a given EC code and return results as a DataFrame.

    Parameters
    ----------
    ec_code : str
        The EC code for the enzyme.
    chebi_ids : List[str], optional
        List of ChEBI IDs for additional context, default is None.
    client : Neo4jClient, optional
        Neo4j client for database interaction, injected via autoclient.

    Returns
    -------
    List[indra.statements.Statement]
        List of INDRA statements representing the analysis results.
    """
    if chebi_ids is None:
        chebi_ids = []

    stmts = metabolomics_explanation(client=client, ec_code=ec_code, chebi_ids=chebi_ids)

    return stmts

def parse_metabolites(metabolites: Iterable[str]) -> Tuple[Dict[str, str], List[str]]:
    """Parse metabolite identifiers to a list of CHEBI IDs."""
    chebi_ids = []
    errors = []
    for entry in metabolites:
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
