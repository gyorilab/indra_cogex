import logging
import re
from typing import Dict, Optional, Union, Tuple, List, Iterable, Collection

import pandas as pd

from indra.databases import hgnc_client
from indra_cogex.client.enrichment.utils import get_statement_metadata_for_pairs
from indra_cogex.client.neo4j_client import autoclient, Neo4jClient
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
    kinase_ora,
)
from indra_cogex.client.enrichment.signed import reverse_causal_reasoning
from indra.databases.hgnc_client import is_kinase, is_transcription_factor
from indra.databases import uniprot_client


logger = logging.getLogger(__name__)


@autoclient()
def discrete_analysis(
    gene_list: List[str],
    method: str = 'fdr_bh',
    alpha: float = 0.05,
    keep_insignificant: bool = False,
    minimum_evidence_count: int = 1,
    minimum_belief: float = 0,
    indra_path_analysis: bool = False,
    background_gene_list: List[str] = None,
    *,
    client: Neo4jClient
) -> Dict[str, pd.DataFrame]:
    """Perform discrete analysis on the provided genes.

    Corresponding web-form based analysis can be found at:
    https://discovery.indra.bio/gene/discrete

    Parameters
    ----------
    gene_list : List[str]
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
    background_gene_list : List[str], optional
        A list of background genes of which the gene list is a part
        to constrain the space of possible genes to consider
        when calculating enrichment statistics.
    client : Neo4jClient, optional
        The Neo4j client, managed automatically by the autoclient decorator.

    Returns
    -------
    Dict[str, pd.DataFrame]
        A dict with results per analysis type in the form of a DataFrame or None
        if an error occurs or no results are found.
    """
    gene_set, errors = parse_gene_list(gene_list)
    if errors:
        logger.warning(f"Failed to parse the following gene identifiers: {', '.join(errors)}")

    background_gene_ids = None
    if background_gene_list:
        background_genes, _ = parse_gene_list(background_gene_list)
        background_gene_ids = list(background_genes)

    results = {}
    for analysis_name, analysis_func in [
        ("go", go_ora),
        ("wikipathways", wikipathways_ora),
        ("reactome", reactome_ora),
        ("phenotype", phenotype_ora),
        ("indra-upstream", indra_upstream_ora),
        ("indra-downstream", indra_downstream_ora)
    ]:
        # Non-INDRA ORAs
        if analysis_name in {"go", "wikipathways", "reactome", "phenotype"}:
            analysis_result = analysis_func(
                client=client, gene_ids=gene_set, method=method, alpha=alpha,
                keep_insignificant=keep_insignificant,
                background_gene_ids=background_gene_ids
            )
            results[analysis_name] = analysis_result

        # INDRA ORAs
        elif indra_path_analysis:
            analysis_result = analysis_func(
                client=client, gene_ids=gene_set, method=method, alpha=alpha,
                keep_insignificant=keep_insignificant,
                minimum_evidence_count=minimum_evidence_count,
                minimum_belief=minimum_belief,
                background_gene_ids=background_gene_ids
            )

            # Extract kinases and TFs from upstream results
            if analysis_name == "indra-upstream" and analysis_result is not None:
                results[analysis_name] = analysis_result  # original upstream
                kinase_results = pd.DataFrame(columns=analysis_result.columns)
                tf_results = pd.DataFrame(columns=analysis_result.columns)

                for _, row in analysis_result.iterrows():
                    if row['curie'].lower().startswith('hgnc:'):
                        gene_name = row['name']
                        if is_kinase(gene_name):
                            kinase_results = pd.concat([kinase_results, pd.DataFrame([row])], ignore_index=True)
                        if is_transcription_factor(gene_name):
                            tf_results = pd.concat([tf_results, pd.DataFrame([row])], ignore_index=True)

                if not kinase_results.empty:
                    results["indra-upstream-kinases"] = kinase_results
                if not tf_results.empty:
                    results["indra-upstream-tfs"] = tf_results
            else:
                results[analysis_name] = analysis_result

    # Enrich INDRA results with statement metadata
    for result_key in results:
        if result_key.startswith("indra-"):
            df = results[result_key]
            if isinstance(df, pd.DataFrame) and not df.empty:
                regulator_gene_pairs = [
                    (row["curie"], gene_id)
                    for _, row in df.iterrows()
                    for gene_id in gene_set
                ]
                is_downstream = result_key == "indra-downstream"
                metadata_map = get_statement_metadata_for_pairs(
                    regulator_gene_pairs,
                    client=client,
                    is_downstream=is_downstream,
                    minimum_belief=minimum_belief,
                    minimum_evidence=minimum_evidence_count
                )
                df["statements"] = df["curie"].map(lambda c: metadata_map.get(c, []))
                results[result_key] = df

    return results


@autoclient()
def signed_analysis(
    positive_genes: List[str],
    negative_genes: List[str],
    alpha: float = 0.05,
    keep_insignificant: bool = False,
    minimum_evidence_count: int = 1,
    minimum_belief: float = 0,
    *,
    client: Neo4jClient
) -> Optional[pd.DataFrame]:
    """Perform signed analysis using reverse causal reasoning

    Corresponding web-form based analysis can be found at:
    https://discovery.indra.bio/gene/signed

    Parameters
    ----------
    positive_genes : List[str]
        List of positive gene identifiers.
    negative_genes : List[str]
        List of negative gene identifiers.
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
    positive_gene_set, pos_errors = parse_gene_list(positive_genes)
    negative_gene_set, neg_errors = parse_gene_list(negative_genes)

    if pos_errors:
        logger.warning(f"Failed to parse positive gene IDs: {', '.join(pos_errors)}")
    if neg_errors:
        logger.warning(f"Failed to parse negative gene IDs: {', '.join(neg_errors)}")

    all_genes = list(positive_gene_set | negative_gene_set)

    results = reverse_causal_reasoning(
        client=client,
        positive_hgnc_ids=positive_gene_set,
        negative_hgnc_ids=negative_gene_set,
        alpha=alpha,
        keep_insignificant=keep_insignificant,
        minimum_evidence_count=minimum_evidence_count,
        minimum_belief=minimum_belief,
    )

    # Attach INDRA statement metadata
    if isinstance(results, pd.DataFrame) and not results.empty:

        # Separate calls for positive and negative genes with filtering
        positive_pairs = [
            (row["curie"], gene_id)
            for _, row in results.iterrows()
            for gene_id in positive_gene_set
        ]

        negative_pairs = [
            (row["curie"], gene_id)
            for _, row in results.iterrows()
            for gene_id in negative_gene_set
        ]

        # Get metadata with appropriate statement type filtering
        pos_metadata = get_statement_metadata_for_pairs(
            positive_pairs,
            client=client,
            is_downstream=False,
            minimum_belief=minimum_belief,
            minimum_evidence=minimum_evidence_count,
            allowed_stmt_types=['Activation', 'IncreaseAmount']  # NEW!
        )

        neg_metadata = get_statement_metadata_for_pairs(
            negative_pairs,
            client=client,
            is_downstream=False,
            minimum_belief=minimum_belief,
            minimum_evidence=minimum_evidence_count,
            allowed_stmt_types=['Inhibition', 'DecreaseAmount']  # NEW!
        )

        # Combine the results
        metadata_map = {}
        for regulator, statements in pos_metadata.items():
            metadata_map.setdefault(regulator, []).extend(statements)
        for regulator, statements in neg_metadata.items():
            metadata_map.setdefault(regulator, []).extend(statements)

        results["statements"] = results["curie"].map(lambda c: metadata_map.get(c, []))

    return results


@autoclient()
def continuous_analysis(
    gene_names: List[str],
    log_fold_change: List[str],
    species: str,
    permutations: int,
    source: str,
    alpha: float = 0.05,
    keep_insignificant: bool = False,
    minimum_evidence_count: int = 1,
    minimum_belief: float = 0,
    *,
    client: Neo4jClient
) -> pd.DataFrame:
    """Perform continuous gene set analysis on gene expression data.

    Corresponding web-form based analysis is found at:
    https://discovery.indra.bio/gene/continuous

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
        A DataFrame containing the results of the specified analysis.
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


@autoclient()
def kinase_analysis(
    phosphosite_list: List[str],
    alpha: float = 0.05,
    keep_insignificant: bool = False,
    background: Optional[Collection[str]] = None,
    minimum_evidence_count: int = 1,
    minimum_belief: float = 0.0,
    *,
    client: Neo4jClient,
) -> pd.DataFrame:
    """Perform over-representation analysis on kinase-phosphosite relationships.

    Corresponding web-form based analysis is found at:
    https://discovery.indra.bio/gene/kinase

    Parameters
    ----------
    phosphosite_list : List[str]
        List of phosphosites in the format "gene-site" or "UniProtID-site" (e.g., "MAPK1-Y187" or "P23443-T412").
    alpha : float, default=0.05
        Significance threshold for ORA.
    keep_insignificant : bool, default=False
        Whether to retain results that are not statistically significant.
    background : Optional[Collection[str]], default=None
        List of phosphosites in the format "gene-site" or "UniProtID-site" for the background set.
    minimum_evidence_count : int, default=1
        Minimum number of supporting edges in the knowledge graph.
    minimum_belief : float, default=0.0
        Minimum belief score for including kinase-phosphosite relationships.
    client : Neo4jClient
        Neo4j client for querying the database.

    Returns
    -------
    :
        DataFrame with columns:
        - curie (kinase ID)
        - name (kinase name)
        - p (p-value)
        - q (adjusted p-value)
    """
    # Parse phosphosites
    parsed_phosphosites, errors = parse_phosphosite_list(
        [tuple(site.split("-")) for site in phosphosite_list]
    )
    if errors:
        logger.error(f"Warning: Skipped invalid phosphosites: {errors}")

    # Parse background if provided
    parsed_background = None
    if background:
        parsed_background, background_errors = parse_phosphosite_list(
            [tuple(site.split("-")) for site in background]
        )
        if background_errors:
            logger.error(f"Warning: Skipped invalid background phosphosites: {background_errors}")

    return kinase_ora(
        client=client,
        phosphosite_ids=parsed_phosphosites,
        background_phosphosite_ids=parsed_background,
        alpha=alpha,
        keep_insignificant=keep_insignificant,
        minimum_evidence_count=minimum_evidence_count,
        minimum_belief=minimum_belief
    )


def parse_gene_list(gene_list: List[str]) -> Tuple[Dict[str, str], List[str]]:
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
            # Handle special case where an outdated symbol
            # corresponds to multiple current HGNC IDs
            if isinstance(hgnc_id, list):
                hgnc_ids.append(hgnc_id[0])
            elif hgnc_id:
                hgnc_ids.append(hgnc_id)
            else:
                errors.append(entry)
    genes = {hgnc_id: hgnc_client.get_hgnc_name(hgnc_id) for hgnc_id in hgnc_ids}
    return genes, errors


def is_valid_gene(gene: str) -> bool:
    """Check if the given identifier is a gene symbol or a UniProt ID."""
    if not isinstance(gene, str) or len(gene) == 0:
        return False

    # UniProt ID pattern
    uniprot_pattern = re.compile(r"^[OPQ][0-9][A-Z0-9]{3}[0-9](-\d+)?$")
    if uniprot_pattern.match(gene):
        return True

    # Gene symbols (allow alphanumeric but must start with a letter)
    gene_pattern = re.compile(r"^[A-Za-z][A-Za-z0-9]*$")
    return bool(gene_pattern.match(gene))


def is_valid_phosphosite(site: str) -> bool:
    """Validate phosphosite format (e.g., S227, T345, Y100)."""
    return isinstance(site, str) and site[0] in {'S', 'T', 'Y', 'H'} and site[1:].isdigit()


def parse_phosphosite_list(
    phosphosite_list: List[Tuple[str, str]]
) -> Tuple[List[Tuple[str, str]], List[str]]:
    """Convert UniProt IDs to gene symbols and validate phosphosites."""
    phosphosites = []
    errors = []

    for identifier, site in phosphosite_list:
        # Convert UniProt ID to gene symbol if needed
        gene = uniprot_client.get_gene_name(identifier) or identifier

        if is_valid_gene(gene) and is_valid_phosphosite(site):
            phosphosites.append((gene, site))
        else:
            errors.append(f"{identifier}:{site}")

    return phosphosites, errors

