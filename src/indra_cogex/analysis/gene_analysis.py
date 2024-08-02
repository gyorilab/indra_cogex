"""Gene-centric analysis."""

from typing import Dict, List, Mapping, Tuple, Union
from pathlib import Path
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


def discrete_analysis(
    genes: Dict[str, str],
    *,
    client,
    method: str = 'fdr_bh',
    alpha: float = 0.05,
    keep_insignificant: bool = False,
    minimum_evidence_count: int = 1,
    minimum_belief: float = 0
) -> Dict:
    """Perform discrete gene set analysis using various enrichment methods.

    Parameters
    ----------
    genes : dict
        A dictionary of HGNC IDs to gene names.
    client : object
        The client object for making API calls.
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
        A dictionary containing results from various analyses.
    """
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

    results = {
        "go_results": go_results,
        "wikipathways_results": wikipathways_results,
        "reactome_results": reactome_results,
        "phenotype_results": phenotype_results,
        "indra_upstream_results": indra_upstream_results,
        "indra_downstream_results": indra_downstream_results
    }

    if not keep_insignificant:
        for key in results:
            results[key] = {k: v for k, v in results[key].items() if v['adjusted_p_value'] <= alpha}

    return results

def signed_analysis(
            positive_genes: Dict[str, str],
            negative_genes: Dict[str, str],
            client,
            alpha: float = 0.05,
            keep_insignificant: bool = False,
            minimum_evidence_count: int = 1,
            minimum_belief: float = 0
    ) -> Dict:
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

    "Apply alpha and keep_insignificant filters"
    filtered_results = [
        r for r in results
        if keep_insignificant or (r['pvalue'] is not None and r['pvalue'] <= alpha)
    ]

    return {"results": filtered_results}


    def continuous_analysis(
            file_path: Union[str, Path],
            gene_name_column: str,
            log_fold_change_column: str,
            species: str,
            permutations: int,
            *,
            client,
            alpha: float = 0.05,
            keep_insignificant: bool = False,
            source: str = 'go',
            minimum_evidence_count: int = 1,
            minimum_belief: float = 0
    ) -> Union[Dict, str]:
     """
     Perform continuous gene set analysis on gene expression data.

     Parameters
     ----------
     client : object
        The client object for making API calls.
     file_path : str or Path
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
    # Convert file_path to Path object if it's a string
    file_path = Path(file_path)

    # Determine the separator based on the file extension
    sep = "," if file_path.suffix.lower() == ".csv" else "\t"

    # Read the input file
    df = pd.read_csv(file_path, sep=sep)

    # Check if we have enough initial data
    if len(df) < 2:
        return "Error: Input file contains insufficient data. At least 2 genes are required."

    # Get scores based on species
    if species == "rat":
        scores = get_rat_scores(df, gene_name_column, log_fold_change_column)
    elif species == "mouse":
        scores = get_mouse_scores(df, gene_name_column, log_fold_change_column)
    elif species == "human":
        scores = get_human_scores(df, gene_name_column, log_fold_change_column)
    else:
        return f"Error: Unknown species: {species}"
        # Debugging: Print scores
    print(f"Scores for {species}: {scores}")

    # Remove any None keys from scores
    scores = {k: v for k, v in scores.items() if k is not None}

    # Check if we have enough valid scores after processing
    if len(scores) < 2:
        return f"Error: Insufficient valid genes after processing. Got {len(scores)} genes, need at least 2."

    if source != 'go':
        return f"Error: Unsupported source: {source}. Only 'go' is currently supported."

    try:
        results = go_gsea(
            client=client,
            scores=scores,
            permutation_num=permutations,
            alpha=alpha,
            keep_insignificant=keep_insignificant,
            minimum_evidence_count=minimum_evidence_count,
            minimum_belief=minimum_belief
        )
    except Exception as e:
        return f"Error in GO GSEA analysis: {str(e)}"

    return results


def continuous_analysis():
    return None