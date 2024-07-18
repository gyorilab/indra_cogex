"""Gene-centric analysis blueprint."""

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

from ...client.enrichment.signed import reverse_casual_reasoning


def discrete_analysis(client, genes: str, method: str, alpha: float, keep_insignificant: bool,
                      minimum_evidence_count: int, minimum_belief: float):
    """Perform discrete gene set analysis using various enrichment methods.

    Args:
    client: The client object for making API calls
    genes (str): A string of gene identifiers
    method (str): The statistical method for multiple testing correction
    alpha (float): The significance level
    keep_insignificant (bool): Whether to keep statistically insignificant results
    minimum_evidence_count (int): Minimum number of evidence required for INDRA analysis
    minimum_belief (float): Minimum belief score for INDRA analysis

    Returns:
    dict: A dictionary containing results from various analyses"""
    genes, errors = parse_genes_field(genes)
    gene_set = set(genes)

    go_results = go_ora(
        client, gene_set, method=method, alpha=alpha, keep_insignificant=keep_insignificant
    )
    wikipathways_results = wikipathways_ora(
        client, gene_set, method=method, alpha=alpha, keep_insignificant=keep_insignificant
    )
    reactome_results = reactome_ora(
        client, gene_set, method=method, alpha=alpha, keep_insignificant=keep_insignificant
    )
    phenotype_results = phenotype_ora(
        gene_set, client=client, method=method, alpha=alpha, keep_insignificant=keep_insignificant
    )

    indra_upstream_results = indra_upstream_ora(
        client, gene_set, method=method, alpha=alpha, keep_insignificant=keep_insignificant,
        minimum_evidence_count=minimum_evidence_count, minimum_belief=minimum_belief
    )
    indra_downstream_results = indra_downstream_ora(
        client, gene_set, method=method, alpha=alpha, keep_insignificant=keep_insignificant,
        minimum_evidence_count=minimum_evidence_count, minimum_belief=minimum_belief
    )

    return {
        "go_results": go_results,
        "wikipathways_results": wikipathways_results,
        "reactome_results": reactome_results,
        "phenotype_results": phenotype_results,
        "indra_upstream_results": indra_upstream_results,
        "indra_downstream_results": indra_downstream_results,
        "errors": errors
    }

    def signed_analysis(client, positive_genes: str, negative_genes: str, alpha: float,
                        keep_insignificant: bool, minimum_evidence_count: int, minimum_belief: float):
         """Perform signed gene set analysis using reverse causal reasoning.

    Args:
    client: The client object for making API calls
    positive_genes (str): A string of gene identifiers for positively regulated genes
    negative_genes (str): A string of gene identifiers for negatively regulated genes
    alpha (float): The significance level
    keep_insignificant (bool): Whether to keep statistically insignificant results
    minimum_evidence_count (int): Minimum number of evidence required
    minimum_belief (float): Minimum belief score required

    Returns:
    dict: A dictionary containing results and any parsing errors"""
    positive_genes, positive_errors = parse_genes_field(positive_genes)
    negative_genes, negative_errors = parse_genes_field(negative_genes)

    results = reverse_causal_reasoning(
        client=client,
        positive_hgnc_ids=positive_genes,
        negative_hgnc_ids=negative_genes,
        alpha=alpha,
        keep_insignificant=keep_insignificant,
        minimum_evidence_count=minimum_evidence_count,
        minimum_belief=minimum_belief,
    )

    return {
        "results": results,
        "positive_errors": positive_errors,
        "negative_errors": negative_errors
    }

    def continuous_analysis(client, file_path: str, gene_name_column: str, log_fold_change_column: str,
                            species: str, permutations: int, alpha: float, keep_insignificant: bool,
                            source: str, minimum_evidence_count: int, minimum_belief: float):
         """Perform continuous gene set analysis on gene expression data.

    Args:
    client: The client object for making API calls
    file_path (str): Path to the input file containing gene expression data
    gene_name_column (str): Name of the column containing gene names
    log_fold_change_column (str): Name of the column containing log fold change values
    species (str): Species of the gene expression data ('rat', 'mouse', or 'human')
    permutations (int): Number of permutations for statistical analysis
    alpha (float): The significance level
    keep_insignificant (bool): Whether to keep statistically insignificant results
    source (str): The type of analysis to perform ('go', 'wikipathways', 'reactome', 'phenotype', 'indra-upstream', 'indra-downstream')
    minimum_evidence_count (int): Minimum number of evidence required for INDRA analysis
    minimum_belief (float): Minimum belief score for INDRA analysis

    Returns:
    The results of the specified analysis"""
    sep = "," if file_path.endswith("csv") else "\t"
    df = pd.read_csv(file_path, sep=sep)

    if species == "rat":
        scores = get_rat_scores(df, gene_symbol_column_name=gene_name_column, score_column_name=log_fold_change_column)
    elif species == "mouse":
        scores = get_mouse_scores(df, gene_symbol_column_name=gene_name_column,
                                  score_column_name=log_fold_change_column)
    elif species == "human":
        scores = get_human_scores(df, gene_symbol_column_name=gene_name_column,
                                  score_column_name=log_fold_change_column)
    else:
        raise ValueError(f"Unknown species: {species}")

    if source == "go":
        results = go_gsea(client=client, scores=scores, permutation_num=permutations, alpha=alpha,
                          keep_insignificant=keep_insignificant)
    elif source == "wikipathways":
        results = wikipathways_gsea(client=client, scores=scores, permutation_num=permutations, alpha=alpha,
                                    keep_insignificant=keep_insignificant)
    elif source == "reactome":
        results = reactome_gsea(client=client, scores=scores, permutation_num=permutations, alpha=alpha,
                                keep_insignificant=keep_insignificant)
    elif source == "phenotype":
        results = phenotype_gsea(client=client, scores=scores, permutation_num=permutations, alpha=alpha,
                                 keep_insignificant=keep_insignificant)
    elif source == "indra-upstream":
        results = indra_upstream_gsea(client=client, scores=scores, permutation_num=permutations, alpha=alpha,
                                      keep_insignificant=keep_insignificant,
                                      minimum_evidence_count=minimum_evidence_count, minimum_belief=minimum_belief)
    elif source == "indra-downstream":
        results = indra_downstream_gsea(client=client, scores=scores, permutation_num=permutations, alpha=alpha,
                                        keep_insignificant=keep_insignificant,
                                        minimum_evidence_count=minimum_evidence_count, minimum_belief=minimum_belief)
    else:
        raise ValueError(f"Unknown source: {source}")

    return results