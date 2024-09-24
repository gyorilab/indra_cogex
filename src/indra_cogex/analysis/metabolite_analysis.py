"""Metabolite-centric analysis."""

from typing import Dict, List
import logging
import pandas as pd
from indra_cogex.client.enrichment.mla import (
    metabolomics_explanation,
    metabolomics_ora,
)
from indra_cogex.client.neo4j_client import Neo4jClient
from statsmodels.stats.multitest import multipletests
from indra_cogex.client.neo4j_client import autoclient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@autoclient()
def metabolite_discrete_analysis(
        metabolites: Dict[str, str],
        method: str = "bonferroni",
        alpha: float = 0.05,
        keep_insignificant: bool = False,
        minimum_evidence_count: int = 1,
        minimum_belief: float = 0.5,
        *,
        client: Neo4jClient  # Client argument moved to the end as a keyword argument
) -> pd.DataFrame:
    """
    Perform discrete metabolite analysis and return results as a DataFrame.

    Parameters
    ----------
    metabolites : Dict[str, str]
        Dictionary of metabolite identifiers (CHEBI IDs).
    method : str, optional
        Method to adjust p-values, default is "bonferroni".
    alpha : float, optional
        Significance level, default is 0.05.
    keep_insignificant : bool, optional
        Whether to retain insignificant results, default is False.
    minimum_evidence_count : int, optional
        Minimum evidence count threshold, default is 1.
    minimum_belief : float, optional
        Minimum belief threshold for filtering results, default is 0.5.
    client : Neo4jClient, optional
        Neo4j client for database interaction, injected via autoclient.

    Returns
    -------
    pd.DataFrame
        DataFrame containing the analysis results.
    """

    chebi_ids = list(metabolites.keys())

    # Perform the metabolomics ORA analysis
    ora_results = metabolomics_ora(
        client=client,
        chebi_ids=chebi_ids,
        method=method,
        alpha=alpha,
        minimum_belief=minimum_belief,
    )

    if ora_results.empty:
        logger.warning("Metabolomics ORA returned empty results.")
        return pd.DataFrame(columns=['curie', 'name', 'p_value', 'adjusted_p_value', 'evidence_count'])

    required_columns = ['curie', 'name', 'p', 'mlp']
    if not all(col in ora_results.columns for col in required_columns):
        missing_columns = [col for col in required_columns if col not in ora_results.columns]
        raise ValueError(f"Missing required columns in metabolomics_ora results: {missing_columns}")
    if 'adjusted_p_value' not in ora_results.columns:
        if method == "bonferroni":
            ora_results['adjusted_p_value'] = ora_results['p'] * len(ora_results)
        elif method == "fdr_bh":
            _, ora_results['adjusted_p_value'], _, _ = multipletests(ora_results['p'], method='fdr_bh')
        else:
            logger.warning(f"Unsupported method '{method}'. Using raw p-values.")
            ora_results['adjusted_p_value'] = ora_results['p']

    # Process and filter the results
    ora_results['evidence_count'] = ora_results['mlp'].apply(
        lambda mlp: int(2 ** mlp) if 'mlp' in ora_results.columns else 0
    )
    ora_results = ora_results[
        (ora_results['adjusted_p_value'] <= alpha) &
        (ora_results['evidence_count'] >= minimum_evidence_count) |
        keep_insignificant
        ]

    return ora_results[['curie', 'name', 'p', 'adjusted_p_value', 'evidence_count']]


@autoclient()
def enzyme_analysis(
        ec_code: str,
        chebi_ids: List[str] = None,
        *,
        client: Neo4jClient  # Client argument moved to the end as a keyword argument
) -> pd.DataFrame:
    """
    Perform enzyme analysis for a given EC code and return results as a DataFrame.

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
    pd.DataFrame
        DataFrame containing enzyme analysis results.
    """
    if chebi_ids is None:
        chebi_ids = []

    stmts = metabolomics_explanation(client=client, ec_code=ec_code, chebi_ids=chebi_ids)

    # Assuming stmts is a list of results, convert it into a DataFrame for consistency
    if not stmts:
        logger.warning(f"No results found for EC code: {ec_code}")
        return pd.DataFrame(columns=['ec_code', 'explanation'])

    return pd.DataFrame(stmts, columns=['ec_code', 'explanation'])
