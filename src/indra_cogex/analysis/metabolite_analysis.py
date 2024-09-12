"""Metabolite-centric analysis."""

from typing import Dict, Any, List, Mapping, Tuple
import logging
import pandas as pd
from indra.databases import chebi_client
from indra_cogex.client.enrichment.mla import (
    EXAMPLE_CHEBI_CURIES,
    metabolomics_explanation,
    metabolomics_ora,
)
from indra_cogex.client.neo4j_client import Neo4jClient
from statsmodels.stats.multitest import multipletests


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def discrete_analysis(
        client: Neo4jClient,
        metabolites: Dict[str, str],
        method: str = "bonferroni",
        alpha: float = 0.05,
        keep_insignificant: bool = False,
        minimum_evidence_count: int = 1,
        minimum_belief: float = 0.5,
) -> Dict[str, Any]:
    """
    Perform discrete metabolite analysis.
    """
    logger.info(f"Starting discrete analysis with {len(metabolites)} metabolites")
    logger.info(
        f"Parameters: method={method}, alpha={alpha}, keep_insignificant={keep_insignificant}, minimum_evidence_count={minimum_evidence_count}, minimum_belief={minimum_belief}")

    # Extract CHEBI IDs from the metabolites dictionary
    chebi_ids = list(metabolites.keys())

    # Perform the metabolomics ORA analysis
    ora_results = metabolomics_ora(
        client=client,
        chebi_ids=chebi_ids,
        method=method,
        alpha=alpha,
        minimum_belief=minimum_belief,
    )

    logger.info(f"Metabolomics ORA returned results shape: {ora_results.shape}")

    if ora_results.empty:
        logger.warning("Metabolomics ORA returned empty results.")
        return {"results": {}, "metabolites": metabolites}

    logger.info(f"Columns in ORA results: {ora_results.columns.tolist()}")

    # Ensure required columns are present
    required_columns = ['curie', 'name', 'p', 'mlp']
    if not all(col in ora_results.columns for col in required_columns):
        missing_columns = [col for col in required_columns if col not in ora_results.columns]
        logger.warning(f"Missing required columns in metabolomics_ora results: {missing_columns}")
        return {"results": {}, "metabolites": metabolites}

    # Calculate adjusted p-value if not present
    if 'adjusted_p_value' not in ora_results.columns:
        logger.info("Calculating adjusted p-values...")
        if method == "bonferroni":
            ora_results['adjusted_p_value'] = ora_results['p'] * len(ora_results)
        elif method == "fdr_bh":
            _, ora_results['adjusted_p_value'], _, _ = multipletests(ora_results['p'], method='fdr_bh')
        else:
            logger.warning(f"Unsupported method '{method}'. Using raw p-values.")
            ora_results['adjusted_p_value'] = ora_results['p']

    # Process the results
    results = {}
    for _, row in ora_results.iterrows():
        curie = row['curie']
        value = {
            'name': row['name'],
            'p_value': row['p'],
            'adjusted_p_value': row['adjusted_p_value'],
            'evidence_count': int(2 ** row['mlp']) if 'mlp' in row else 0
        }

        if (keep_insignificant or value['adjusted_p_value'] <= alpha) and \
                value['evidence_count'] >= minimum_evidence_count:
            results[curie] = value

    logger.info(f"Analysis complete. Found {len(results)} significant results.")
    return {
        "results": results,
        "metabolites": metabolites,
    }


def enzyme_analysis(
        client: Neo4jClient,  # Specify the type of client here
        ec_code: str,
        chebi_ids: List[str] = None
) -> List:
    """Perform enzyme analysis and explanation for given EC code and optional ChEBI IDs.

    Parameters
    ----------
    client : object
        The client object for making API calls.
    ec_code : str
        The EC code for the enzyme.
    chebi_ids : List[str], optional
        List of ChEBI IDs for additional context.

    Returns
    -------
    List
        A list of statements explaining the enzyme's function."""
    if chebi_ids is None:
        chebi_ids = []
    stmts = metabolomics_explanation(
        client=client, ec_code=ec_code, chebi_ids=chebi_ids
    )
    return stmts


