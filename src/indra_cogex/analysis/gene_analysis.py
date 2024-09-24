import logging
from typing import Dict, Optional, Union, Tuple, List, Iterable

import pandas as pd
from pandas import DataFrame

from indra.databases import hgnc_client
from indra_cogex.client.neo4j_client import autoclient

from indra_cogex.client.neo4j_client import Neo4jClient
from indra_cogex.client.enrichment.continuous import (
    get_human_scores,
    get_mouse_scores,
    get_rat_scores,
    go_gsea,
    wikipathways_gsea,
    phenotype_gsea,
    indra_upstream_gsea,
    indra_downstream_gsea,
    reactome_gsea
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
        genes: List[str],
        method: str = 'fdr_bh',
        alpha: float = 0.05,
        keep_insignificant: bool = False,
        minimum_evidence_count: int = 1,
        minimum_belief: float = 0,
        indra_path_analysis: bool = False,
        *,
        client: Neo4jClient
) -> Dict[str, Union[pd.DataFrame, None]]:
    """
    Perform discrete analysis on the provided genes.

    Parameters
    ----------
    genes : List[str]
        A list of gene identifiers. Can be HGNC symbols or identifiers.
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
    indra_path_analysis : bool, optional
        Whether to perform INDRA pathway analysis, by default False.
    client : Neo4jClient, optional
        The Neo4j client, managed automatically by the autoclient decorator.

    Returns
    -------
    Dict[str, pd.DataFrame | None]
        A dict with results per analysis type in the form of a DataFrame or None
        if an error occurs or no results are found.
    """
    gene_set = parse_gene_list(genes)

    results = {}
    for analysis_name, analysis_func in [
        ("go", go_ora),
        ("wikipathways", wikipathways_ora),
        ("reactome", reactome_ora),
        ("phenotype", phenotype_ora),
        ("indra-upstream", indra_upstream_ora),
        ("indra-downstream", indra_downstream_ora)
    ]:
        # Run non-INDRA analysis
        if analysis_name in {"go", "wikipathways", "reactome", "phenotype"}:
            analysis_result = analysis_func(
                client=client, gene_ids=gene_set, method=method, alpha=alpha,
                keep_insignificant=keep_insignificant
            )
        else:
            # Run INDRA analysis if enabled
            if indra_path_analysis:
                analysis_result = analysis_func(
                    client=client, gene_ids=gene_set, method=method, alpha=alpha,
                    keep_insignificant=keep_insignificant,
                    minimum_evidence_count=minimum_evidence_count,
                    minimum_belief=minimum_belief
                )
            else:
                analysis_result = None

        results[analysis_name] = analysis_result

    return results


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
            keep_insignificant=keep_insignificant,
            minimum_evidence_count=minimum_evidence_count,
            minimum_belief=minimum_belief,
        )

        return results
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
        source: str,
        alpha: float = 0.05,
        keep_insignificant: bool = False,
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
    source : str, optional
        The type of analysis to perform. Should be one of 'go', 'reactome',
        'wikipathways', 'phenotype', 'indra-upstream', or 'indra-downstream'.
    client : Neo4jClient
        The client object for making API calls.
    alpha : float, optional
        The significance level. Defaults to 0.05.
    keep_insignificant : bool, optional
        Whether to keep statistically insignificant results. Defaults to False.
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

    analysis_functions = {
        "go": go_gsea,
        "wikipathways": wikipathways_gsea,
        "reactome": reactome_gsea,
        "phenotype": phenotype_gsea,
        "indra-upstream": indra_upstream_gsea,
        "indra-downstream": indra_downstream_gsea,
    }

    kwargs = dict(
        client=client,
        permutation_num=permutations,
        alpha=alpha,
        keep_insignificant=keep_insignificant,
    )
    if source in ["indra-upstream", "indra-downstream"]:
        kwargs["minimum_evidence_count"] = minimum_evidence_count
        kwargs["minimum_belief"] = minimum_belief

    if species not in score_functions:
        raise ValueError(
            f"Unknown species: {species}. Must be one of 'rat', 'mouse', or 'human'."
        )

    if source not in analysis_functions:
        raise ValueError(
            f"Unknown source: {source}. Must be one of 'go', 'reactome', "
            f"'wikipathways', 'phenotype', 'indra-upstream', or 'indra-downstream'."
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

    kwargs["scores"] = scores

    func = analysis_functions[source]
    result = func(**kwargs)

    return result


def parse_gene_list(gene_list: Iterable[str]) -> Tuple[Dict[str, str], List[str]]:
    """Parse gene list"""
    hgnc_ids = []
    errors = []
    for entry in gene_list:
        if entry.lower().startswith("hgnc:"):
            hgnc_ids.append(entry.lower().replace("hgnc:", "", 1))
        elif entry.isnumeric():
            hgnc_ids.append(entry)
        else:  # probably a symbol
            hgnc_id = hgnc_client.get_current_hgnc_id(entry)
            if hgnc_id:
                hgnc_ids.append(hgnc_id)
            else:
                errors.append(entry)
    genes = {hgnc_id: hgnc_client.get_hgnc_name(hgnc_id) for hgnc_id in hgnc_ids}
    return genes, errors
