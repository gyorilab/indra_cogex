import logging
from typing import Dict, Optional

import pandas as pd
from pandas import DataFrame
from indra_cogex.client.neo4j_client import autoclient

from indra_cogex.client.neo4j_client import Neo4jClient
from indra_cogex.client.enrichment.continuous import (
    get_human_scores,
    get_mouse_scores,
    get_rat_scores,
    go_gsea
)
from indra_cogex.client.enrichment.discrete import (
    go_ora,
    indra_downstream_ora,
    indra_upstream_ora,
    phenotype_ora,
    reactome_ora,
    wikipathways_ora,
)
from indra_cogex.client.enrichment.signed import reverse_causal_reasoning

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@autoclient()
def discrete_analysis(
        genes: Dict[str, str],
        method: str = 'fdr_bh',
        alpha: float = 0.05,
        keep_insignificant: bool = False,
        minimum_evidence_count: int = 1,
        minimum_belief: float = 0,
        *,
        client: Neo4jClient
) -> Optional[pd.DataFrame]:
    """
    Perform discrete analysis on the provided genes.

    Parameters
    ----------
    genes : dict of str
        Dictionary of gene identifiers.
    method : str, optional
        Statistical method to apply, by default 'fdr_bh'.
    alpha : float, optional
        Significance level, by default 0.05.
    keep_insignificant : bool, optional
        Whether to retain insignificant results, by default False.
    minimum_evidence_count : int, optional
        Minimum number of evidence for inclusion, by default 1.
    minimum_belief : float, optional
        Minimum belief score for filtering, by default 0.
    client : Neo4jClient, optional
        The Neo4j client, managed automatically by the autoclient decorator.

    Returns
    -------
    pd.DataFrame or None
        A DataFrame containing analysis results, or None if an error occurs.
    """
    gene_set = set(genes.keys())
    print(f"Gene set: {gene_set}")

    try:
        results = {}
        for analysis_name, analysis_func in [
            ("GO", go_ora),
            ("WikiPathways", wikipathways_ora),
            ("Reactome", reactome_ora),
            ("Phenotype", phenotype_ora),
            ("INDRA Upstream", indra_upstream_ora),
            ("INDRA Downstream", indra_downstream_ora)
        ]:
            if analysis_name in ["GO", "WikiPathways", "Reactome", "Phenotype"]:
                analysis_result = analysis_func(
                    client=client, gene_ids=gene_set, method=method, alpha=alpha,
                    keep_insignificant=keep_insignificant
                )
            else:  # INDRA analyses
                analysis_result = analysis_func(
                    client=client, gene_ids=gene_set, method=method, alpha=alpha,
                    keep_insignificant=keep_insignificant,
                    minimum_evidence_count=minimum_evidence_count,
                    minimum_belief=minimum_belief
                )
            results[analysis_name] = analysis_result

        df_list = []
        for analysis_name, result in results.items():
            df = pd.DataFrame(result)
            df['Analysis'] = analysis_name
            df_list.append(df)

        final_df = pd.concat(df_list, ignore_index=True)
        print(f"Final DataFrame head:\n{final_df.head()}")

        final_df = pd.concat(df_list, ignore_index=True)
        logger.info(f"Final DataFrame shape: {final_df.shape}")
        return final_df
    except Exception as e:
        logger.error(f"An error occurred during discrete analysis: {str(e)}", exc_info=True)
        return None


@autoclient()
def signed_analysis(
    positive_genes: Dict[str, str],
    negative_genes: Dict[str, str],
    alpha: float = 0.05,
    keep_insignificant: bool = False,
    minimum_evidence_count: int = 1,
    minimum_belief: float = 0,
    *,
    client: Neo4jClient
) -> Optional[pd.DataFrame]:
    """
    Perform signed analysis on the provided genes using reverse causal reasoning.

    Parameters
    ----------
    positive_genes : dict of str
        Dictionary of positive gene identifiers.
    negative_genes : dict of str
        Dictionary of negative gene identifiers.
    alpha : float, optional
        Significance level, by default 0.05.
    keep_insignificant : bool, optional
        Whether to retain insignificant results, by default False.
    minimum_evidence_count : int, optional
        Minimum number of evidence for inclusion, by default 1.
    minimum_belief : float, optional
        Minimum belief score for filtering, by default 0.
    client : Neo4jClient, optional
        The Neo4j client, managed automatically by the autoclient decorator.

    Returns
    -------
    pd.DataFrame or None
        A DataFrame containing analysis results, or None if an error occurs.
    """

    try:
        results = reverse_causal_reasoning(
            client=client,
            positive_hgnc_ids=positive_genes,
            negative_hgnc_ids=negative_genes,
            alpha=alpha,
            keep_insignificant=True,  # Always keep all results
            minimum_evidence_count=minimum_evidence_count,
            minimum_belief=minimum_belief,
        )
        print(f"Reverse causal reasoning results: {results}")

        final_df = pd.DataFrame(results)
        print(f"Final DataFrame head:\n{final_df.head()}")

        return final_df
    except Exception as e:
        print(f"An error occurred during signed analysis: {str(e)}")
        logger.exception(e)
        return None


@autoclient()
def continuous_analysis(
        gene_names: str,
        log_fold_change: str,
        species: str,
        permutations: int,
        alpha: float = 0.05,
        keep_insignificant: bool = False,
        source: str = 'go',
        minimum_evidence_count: int = 1,
        minimum_belief: float = 0,
        *,
        client: Neo4jClient
) -> Optional[DataFrame]:
    """
    Perform continuous gene set analysis on gene expression data.

    Parameters
    ----------
    gene_names : list[str]
        Name of the column containing gene names.
    log_fold_change : list[float]
        Name of the column containing log fold change values.
    species : str
        Species of the gene expression data. Should be one of 'rat', 'mouse', or 'human'.
    permutations : int
        Number of permutations for statistical analysis.
    client : Neo4jClient
        The client object for making API calls.
    alpha : float, optional
        The significance level. Defaults to 0.05.
    keep_insignificant : bool, optional
        Whether to keep statistically insignificant results. Defaults to False.
    source : str, optional
        The type of analysis to perform. Defaults to 'go'.
    minimum_evidence_count : int, optional
        Minimum number of evidence required for INDRA analysis. Defaults to 1.
    minimum_belief : float, optional
        Minimum belief score for INDRA analysis. Defaults to 0.

    Returns
    -------
    DataFrame or None
        A DataFrame containing the results of the specified analysis,
        or None if an error occurred.
    """

    score_functions = {
        "rat": get_rat_scores,
        "mouse": get_mouse_scores,
        "human": get_human_scores
    }

    if species not in score_functions:
        raise ValueError(
            f"Unknown species: {species}. Must be one of 'rat', 'mouse', or 'human'."
        )

    if len(gene_names) != len(log_fold_change):
        raise ValueError("Gene names and log fold change values must have the same length.")

    gene_name_column_name = "genes"
    log_fold_change_column_name = "log_fold_change"

    df = pd.DataFrame({
        gene_name_column_name: gene_names,
        log_fold_change_column_name: log_fold_change
    })

    scores = score_functions[species](
        df, gene_name_column_name, log_fold_change_column_name
    )
    scores = {k: v for k, v in scores.items() if k is not None}

    if len(scores) < 2:
        raise ValueError(f"Insufficient valid genes after processing. Got {len(scores)} genes, need at least 2.")

    if source != 'go':
        raise ValueError(f"Unsupported source: {source}. Only 'go' is currently supported.")

    results = go_gsea(
        client=client,
        scores=scores,
        permutation_num=permutations,
        alpha=alpha,
        keep_insignificant=keep_insignificant,
        minimum_evidence_count=minimum_evidence_count,
        minimum_belief=minimum_belief
    )
    return pd.DataFrame(results)
