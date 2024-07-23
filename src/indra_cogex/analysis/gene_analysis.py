"""Gene-centric analysis."""

from typing import Dict, List, Mapping, Tuple
import pandas as pd

from indra.databases import hgnc_client
from indra_cogex.client.enrichment.continuous import (
    get_human_scores,
    get_mouse_scores,
    get_rat_scores,
    indra_downstream_gsea,
    indra_upstream_gsea,
    phenotype_gsea,
    reactome_gsea,
    wikipathways_gsea,
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


def discrete_analysis(client, genes: Dict[str, str], method: str, alpha: float,
                      keep_insignificant: bool, minimum_evidence_count: int,
                      minimum_belief: float) -> Dict:
    """Perform discrete gene set analysis using various enrichment methods.

    Parameters
    ----------
    client : object
        The client object for making API calls.
    genes : dict
        A dictionary of HGNC IDs to gene names.
    method : str
        The statistical method for multiple testing correction.
    alpha : float
        The significance level.
    keep_insignificant : bool
        Whether to keep statistically insignificant results.
    minimum_evidence_count : int
        Minimum number of evidence required for INDRA analysis.
    minimum_belief : float
        Minimum belief score for INDRA analysis.

    Returns
    -------
    dict
        A dictionary containing results from various analyses."""
    gene_set = set(genes.keys())

    go_results = go_ora(
        client, gene_set, method=method, alpha=alpha,
        keep_insignificant=keep_insignificant
    )
    wikipathways_results = wikipathways_ora(
        client, gene_set, method=method, alpha=alpha,
        keep_insignificant=keep_insignificant
    )
    reactome_results = reactome_ora(
        client, gene_set, method=method, alpha=alpha,
        keep_insignificant=keep_insignificant
    )
    phenotype_results = phenotype_ora(
        gene_set, client=client, method=method, alpha=alpha,
        keep_insignificant=keep_insignificant
    )
    indra_upstream_results = indra_upstream_ora(
        client, gene_set, method=method, alpha=alpha,
        keep_insignificant=keep_insignificant,
        minimum_evidence_count=minimum_evidence_count,
        minimum_belief=minimum_belief
    )
    indra_downstream_results = indra_downstream_ora(
        client, gene_set, method=method, alpha=alpha,
        keep_insignificant=keep_insignificant,
        minimum_evidence_count=minimum_evidence_count,
        minimum_belief=minimum_belief
    )

    return {
        "go_results": go_results,
        "wikipathways_results": wikipathways_results,
        "reactome_results": reactome_results,
        "phenotype_results": phenotype_results,
        "indra_upstream_results": indra_upstream_results,
        "indra_downstream_results": indra_downstream_results
    }


def signed_analysis(client, positive_genes: Dict[str, str],
                    negative_genes: Dict[str, str], alpha: float,
                    keep_insignificant: bool, minimum_evidence_count: int,
                    minimum_belief: float) -> Dict:
    """Perform signed gene set analysis using reverse causal reasoning.

    Parameters
    ----------
    client : object
        The client object for making API calls.
    positive_genes : dict
        A dictionary of HGNC IDs to gene names for positively regulated genes.
    negative_genes : dict
        A dictionary of HGNC IDs to gene names for negatively regulated genes.
    alpha : float
        The significance level.
    keep_insignificant : bool
        Whether to keep statistically insignificant results.
    minimum_evidence_count : int
        Minimum number of evidence required.
    minimum_belief : float
        Minimum belief score required.

    Returns
    -------
    dict
        A dictionary containing results from the analysis."""
    results = reverse_causal_reasoning(
        client=client,
        positive_hgnc_ids=positive_genes,
        negative_hgnc_ids=negative_genes,
        alpha=alpha,
        keep_insignificant=keep_insignificant,
        minimum_evidence_count=minimum_evidence_count,
        minimum_belief=minimum_belief,
    )

    return {"results": results}


def continuous_analysis(
    client,
    file_path: str,
    gene_name_column: str,
    log_fold_change_column: str,
    species: str,
    permutations: int,
    alpha: float,
    keep_insignificant: bool,
    source: str,
    minimum_evidence_count: int,
    minimum_belief: float
) -> Union[Dict, str]:
    """
    Perform continuous gene set analysis on gene expression data.

    Parameters
    ----------
    client : object
        The client object for making API calls.
    file_path : str
        Path to the input file containing gene expression data.
    gene_name_column : str
        Name of the column containing gene names.
    log_fold_change_column : str
        Name of the column containing log fold change values.
    species : str
        Species of the gene expression data ('rat', 'mouse', or 'human').
    permutations : int
        Number of permutations for statistical analysis.
    alpha : float
        The significance level.
    keep_insignificant : bool
        Whether to keep statistically insignificant results.
    source : str
        The type of analysis to perform.
    minimum_evidence_count : int
        Minimum number of evidence required for INDRA analysis.
    minimum_belief : float
        Minimum belief score for INDRA analysis.

    Returns
    -------
    Union[Dict, str]
        A dictionary containing the results of the specified analysis,
        or a string containing an error message if the analysis fails.
    """
    sep = "," if file_path.endswith("csv") else "\t"
    df = pd.read_csv(file_path, sep=sep)

    # Ensure we have at least two valid entries
    df = df.dropna(subset=[gene_name_column, log_fold_change_column])
    if len(df) < 2:
        return ("Error: Insufficient valid data for analysis. "
                "At least 2 genes with non-null values are required.")

    if species == "rat":
        scores = get_rat_scores(
            df,
            gene_symbol_column_name=gene_name_column,
            score_column_name=log_fold_change_column
        )
    elif species == "mouse":
        scores = get_mouse_scores(
            df,
            gene_symbol_column_name=gene_name_column,
            score_column_name=log_fold_change_column
        )
    elif species == "human":
        scores = get_human_scores(
            df,
            gene_symbol_column_name=gene_name_column,
            score_column_name=log_fold_change_column
        )
    else:
        return f"Error: Unknown species: {species}"

    # Ensure we have at least two scores after processing
    if len(scores) < 2:
        return ("Error: Insufficient data after processing. "
                "At least 2 valid genes are required.")

    analysis_functions = {
        "go": go_gsea,
        "wikipathways": wikipathways_gsea,
        "reactome": reactome_gsea,
        "phenotype": phenotype_gsea,
        "indra-upstream": indra_upstream_gsea,
        "indra-downstream": indra_downstream_gsea
    }

    if source not in analysis_functions:
        return f"Error: Unknown source: {source}"

    try:
        results = analysis_functions[source](
            client=client,
            scores=scores,
            permutation_num=permutations,
            alpha=alpha,
            keep_insignificant=keep_insignificant,
            minimum_evidence_count=minimum_evidence_count,
            minimum_belief=minimum_belief
        )
    except Exception as e:
        return f"Error in {source} analysis: {str(e)}"

    return results