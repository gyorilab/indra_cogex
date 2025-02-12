"""
This module implements analysis of mechanisms connecting a source with a set of
downstream targets to construct possible explanations from the INDRA CoGEx
knowledge graph.

Possible explanations considered include INDRA statements, pathway membership,
determining if any of the proteins belong to the same protein family/complex
as the target, and using gene set enrichment on intermediates between
the source and the target.
"""
# Standard library imports
import os
import json
import base64
import logging
import itertools
from collections import defaultdict
from typing import List, Tuple, Optional, Dict

# Third-party imports
import pandas as pd
import matplotlib

matplotlib.use('agg')
import matplotlib.pyplot as plt

# INDRA imports
from indra.databases import hgnc_client
from indra.assemblers.html import HtmlAssembler
from indra.statements import *

# Local imports
from indra_cogex.client import *
from ..client.enrichment.discrete import (
    indra_upstream_ora,
    go_ora,
    reactome_ora,
    wikipathways_ora
)
from .gene_analysis import discrete_analysis

logger = logging.getLogger(__name__)


def get_valid_gene_id(gene_name):
    """Return HGNC id for a gene name handling outdated symbols.

    Parameters
    ----------
    gene_name : str
        The gene name to get the HGNC id for.

    Returns
    -------
    hgnc_id : str
        The HGNC id corresponding ton the gene name.
    """
    # Get ID for gene name
    hgnc_id = hgnc_client.get_hgnc_id(gene_name)

    # Try to turn an outdated symbol into a valid one
    # if possible
    if not hgnc_id:
        hgnc_id = hgnc_client.get_current_hgnc_id(gene_name)
        if isinstance(hgnc_id, list):
            hgnc_id = hgnc_id[0]
        elif not hgnc_id:
            logger.warning("%s is not a valid gene name" % gene_name)
            return None

    return hgnc_id


def get_valid_gene_ids(gene_names):
    """Return valid HGNC ids for all genes in the list.

    Any gene names that cannot be converted to HGNC ids are ignored.

    Parameters
    ----------
    gene_names : list
        Contains proteins user enters to analyze in relation to source

    Returns
    -------
    hgnc_ids : list
        HGNC ids for the input gene names
    """
    hgnc_ids = []
    for gene_name in gene_names:
        hgnc_id = get_valid_gene_id(gene_name)
        if hgnc_id:
            hgnc_ids.append(hgnc_id)

    return hgnc_ids


@autoclient()
def get_stmts_from_source(source_id, *, client, source_ns='HGNC', target_proteins=None):
    """To get a dataframe of proteins that the target protien has direct INDRA
    relationship with to find the stmt_jsons, type, and id

    Parameters
    ----------
    source_ns
    client
    source_id : string
        The protein of interest in relation to protien list user enters

    target_proteins : list
        Contains proteins user enters to analyze in relation to target

    Returns
    -------
    stmts_by_protein_df : Dataframe
        Unfiltered dataframe that contains all INDRA relationships for
        target protein
    stmts_by_protein_filtered_df : dataframe
        Contains INDRA relationships for source protein filtered by
        "target_proteins"
    """
    # Get indra_rel objects for protiens that have a direct INDRA relationship
    # with the source protein
    res = client.get_target_relations(
        source=(source_ns, source_id),
        relation='indra_rel',
        source_type='BioEntity',
        target_type='BioEntity',
    )
    # TODO: we should look up additional evidence for these
    # statements and add them here

    # Extract necessary information from the result and creates dictionary
    records = [
        {
            "name": entry.target_name,
            "stmt_json": entry.data["stmt_json"],
            "target_type": entry.target_ns,
            "target_id": entry.target_id,
            "stmt_type": entry.data["stmt_type"],
            "evidence_count": entry.data["evidence_count"],
            "stmt_hash": entry.data["stmt_hash"]
        }
        for entry in res
    ]

    stmts_by_protein_df = pd.DataFrame.from_records(records)

    # If there are target proteins filters data frame based on that list
    if target_proteins:
        stmts_by_protein_filtered_df = stmts_by_protein_df[
            stmts_by_protein_df.target_id.isin(target_proteins)]

        evidences = []
        for hashes in stmts_by_protein_filtered_df["stmt_hash"].values:
            evidences.append(get_evidences_for_stmt_hash(int(hashes)))
        stmts_by_protein_filtered_df_copy = stmts_by_protein_filtered_df.copy()
        stmts_by_protein_filtered_df_copy["evidences"] = evidences
        logger.info("Dataframe of protiens that have INDRA relationships with source\
                    that have been filtered:\n" + str(stmts_by_protein_filtered_df_copy))

    else:
        stmts_by_protein_filtered_df_copy = stmts_by_protein_df

    return stmts_by_protein_df, stmts_by_protein_filtered_df_copy


def plot_stmts_by_type(stmts_df):
    """Visualize frequency of interaction types among proteins that have direct
       INDRA relationship to source

    Parameters
    ----------
    stmts_df : pd.DataGrame
        Contains INDRA statements represented as a data frame..
    """
    # Plot bar chart based on "stmt_type" which are the interaction types
    fig, ax = plt.subplots(figsize=(10, 6))
    type_counts = stmts_df["stmt_type"].value_counts()
    type_counts.plot.bar(ax=ax)
    ax.set_xlabel("Interaction Type")
    ax.set_ylabel("Frequency")
    ax.set_title("Frequency of Type of Interaction With Target")
    return fig


def assemble_protein_stmt_htmls(stmts_df):
    """Assemble HTML page for each protein's INDRA statements in a data frame.

    Parameters
    ----------
    stmts_df : pd.DataFrame
        Contains INDRA relationships for source protein filtered by
        "target_proteins" genes
    """

    stmts_by_protein = defaultdict(list)
    for _, row in stmts_df.iterrows():
        stmt = stmt_from_json(json.loads(row['stmt_json']))
        stmts_by_protein[row['name']].append(stmt)

    html_content = {}
    for name, stmts in stmts_by_protein.items():
        ha = HtmlAssembler(stmts, title=f'Statements for {name}',
                           db_rest_url='https://db.indra.bio')
        html_content[name] = ha.make_model()

    return html_content


def shared_pathways_between_gene_sets(source_hgnc_ids, target_hgnc_ids):
    """Find shared pathways between list of target genes and source protien

    Parameters
    ----------
    target_hgnc_ids : list
        HGNC ids for a source set
    source_hgnc_ids : list
        HGNC ids for a target set

    Returns
    -------
    shared_pathways_list : list
        Nested list of Relation objects describing the pathways shared for
        a given pair of genes.
    """
    shared_pathways_list = []
    for source_id, target_id in itertools.product(source_hgnc_ids, target_hgnc_ids):
        result = get_shared_pathways_for_genes((
            ("HGNC", target_id), ("HGNC", source_id)))
        if result:
            shared_pathways_list.append(result)
    if not shared_pathways_list:
        logger.info("There are no shared pathways between the "
                    "source and targets")
    return shared_pathways_list


@autoclient()
def shared_protein_families(target_hgnc_ids, source_hgnc_id, *, client):
    """Determine if any gene in gene list isa/partof the source protein
    Parameters
    ----------
    target_hgnc_ids : list
        Contains HGNC ids for target list
    source_hgnc_id : string
        The source proteins HGNC id

    Returns
    -------
    shared_families_df : dataframe
        Contains shared protein family complexes for the target proteins and
        the source
    """
    # adds hgnc: to the beginning of source id to format for query
    source_hgnc_id = "hgnc:" + source_hgnc_id

    # iterates through target ids to format for query
    targets_list = []
    for ids in target_hgnc_ids:
        target_id = "hgnc:" + ids
        targets_list.append(target_id)
    target_ids = str(targets_list)

    # if the list is too long \n would appear so removes it
    # adds commas to blank spaces to format for cypher
    if "\n" in target_ids:
        target_ids = target_ids.replace("\n", "").replace(" ", ",")

    # query to return protein family complexes for the targets and source
    cypher = f"""MATCH (target_proteins:BioEntity)-[:isa|partof*1..]->(family1:BioEntity)
    WHERE target_proteins.id in {target_ids}
    WITH collect(family1) AS targets_members

    MATCH (source_protein:BioEntity)-[:isa|partof*1..]->(family2:BioEntity)
    WHERE source_protein.id = '{source_hgnc_id}'
    WITH collect(family2) AS source_members, targets_members

    RETURN source_members, targets_members """

    results = client.query_tx(cypher)

    # if the query returned results
    if results:
        # if both the source and target return results
        if results[0][0] and results[0][1]:
            # saves protein complexes into dataframes
            source_df = pd.DataFrame(results[0][0])
            target_df = pd.DataFrame(results[0][1])
            # creates new dataframe for shared complexes
            shared_families_df = target_df[target_df.id.isin(source_df["id"].values)]
            return shared_families_df


def get_go_terms_for_source(source_hgnc_id):
    """This method gets the go terms for the source protein

    Parameters
    ----------
    source_hgnc_id : string
        HGNC id for the source protein

    Returns
    -------
    source_go_terms : list
        Contains the GO terms for source proteins
    go_nodes : list
        List of node objects that has information about GO terms for source
    """
    # these are the GO terms for target protein
    go_nodes = get_go_terms_for_gene(("HGNC", source_hgnc_id))
    source_go_terms = [
        go_node.db_id.lower() for go_node in go_nodes
    ]

    return source_go_terms, go_nodes


@autoclient()
def shared_upstream_bioentities_from_targets(
    stmts_by_protein_df: pd.DataFrame,
    target_genes: List[str],
    *,
    client
) -> Tuple[List[str], pd.DataFrame]:
    """Get upstream molecules intersecting with bioentities.

    Parameters
    ----------
    stmts_by_protein_df : pd.DataFrame
        DataFrame containing INDRA statements
    target_genes : List[str]
        List of target gene symbols
    client :
        The client instance

    Returns
    -------
    Tuple[List[str], pd.DataFrame]
        Shared proteins and detailed analysis results
    """
    # Get upstream analysis from database
    upstream_df = indra_upstream_ora(
        client=client,
        gene_ids=target_genes
    )

    # Find shared proteins
    shared_proteins = list(set(upstream_df["name"].values).intersection(
        set(stmts_by_protein_df["name"].values)))

    if shared_proteins:
        shared_entities = upstream_df[upstream_df.name.isin(shared_proteins)]
        logger.info("Found shared upstream bioentities")
    else:
        logger.info("No shared upstream bioentities found")
        shared_entities = pd.DataFrame()

    return shared_proteins, shared_entities


@autoclient()
def find_shared_go_terms(source_go_terms: List[str], target_genes: List[str], *, client) -> Optional[pd.DataFrame]:
    """Finds shared GO terms between the gene list and source protein's GO terms.

    Parameters
    ----------
    source_go_terms : List[str]
        GO terms for source protein
    target_genes : List[str]
        Target gene symbols
    client :
        The client instance

    Returns
    -------
    Optional[pd.DataFrame]
        DataFrame with shared GO terms, or None if no shared terms found
    """
    # Get GO terms data from database
    go_df = go_ora(
        client=client,
        gene_ids=target_genes
    )

    # Find shared terms
    shared_go = list(set(go_df["curie"].values).intersection(set(source_go_terms)))

    if shared_go:
        shared_go_df = go_df[go_df.curie.isin(shared_go)]
        logger.info("Found shared GO terms between source and targets")
        return shared_go_df

    logger.info("No shared GO terms found between source and targets")
    return None


@autoclient()
def combine_target_gene_pathways(source_id: str, target_ids: List[str], *, client) -> pd.DataFrame:
    """Creates combined dataframe of REACTOME and Wikipathways pathway data.

    Parameters
    ----------
    source_id : str
        HGNC ID for source gene
    target_ids : List[str]
        List of HGNC IDs for target genes
    client :
        The client instance

    Returns
    -------
    pd.DataFrame
        Combined pathway information from REACTOME and WikiPathways
    """
    # Get all gene IDs
    all_genes = [source_id] + target_ids

    # Get pathway data using database calls
    reactome_df = reactome_ora(
        client=client,
        gene_ids=all_genes
    )

    wiki_df = wikipathways_ora(
        client=client,
        gene_ids=all_genes
    )

    # Combine results
    pathways_df = pd.concat([reactome_df, wiki_df])

    return pathways_df


def graph_boxplots(shared_go_df, shared_entities):
    """Create boxplots to visualize p and q values.

    Parameters
    ----------
    shared_go_df : pd.DataFrame
        DataFrame with GO terms analysis
    shared_entities : pd.DataFrame
        DataFrame with bioentities analysis

    Returns
    -------
    str
        Base64 encoded string of the plot
    """
    fig, axs = plt.subplots(2, 2, figsize=(12, 8))

    if not shared_go_df.empty:
        axs[0, 0].set_title("P-values for Shared Go Terms")
        shared_go_df.boxplot(column=["p"], ax=axs[0, 0])

        axs[0, 1].set_title("Q-values for Shared Go Terms")
        shared_go_df.boxplot(column=["q"], ax=axs[0, 1])

    if not shared_entities.empty:
        axs[1, 0].set_title("P-values for Shared Bioentities")
        shared_entities.boxplot(column=["p"], ax=axs[1, 0])

        axs[1, 1].set_title("Q-values for Shared Bioentities")
        shared_entities.boxplot(column=["q"], ax=axs[1, 1])

    plt.tight_layout()

    # Use temporary file instead of BytesIO
    from tempfile import NamedTemporaryFile
    with NamedTemporaryFile(suffix='.png') as tmpfile:
        fig.savefig(tmpfile.name, format='png', bbox_inches='tight')
        with open(tmpfile.name, 'rb') as f:
            plot_data = base64.b64encode(f.read()).decode('utf-8')

    plt.close(fig)
    return plot_data


def convert_plot_to_base64(fig):
    """Helper function to convert matplotlib figure to base64 string."""
    from tempfile import NamedTemporaryFile

    # Save to temporary file
    with NamedTemporaryFile(suffix='.png') as tmpfile:
        fig.savefig(tmpfile.name, format='png', bbox_inches='tight')
        # Read and encode
        with open(tmpfile.name, 'rb') as f:
            plot_data = base64.b64encode(f.read()).decode('utf-8')

    plt.close(fig)
    return plot_data


@autoclient()
def run_explain_downstream_analysis(source_hgnc_id, target_hgnc_ids, *, client):
    """Run complete downstream analysis.

    Parameters
    ----------
    source_hgnc_id : str
        HGNC ID for the source gene
    target_hgnc_ids : list
        List of HGNC IDs for target genes
    client :
        The client instance

    Returns
    -------
    dict
        Dictionary containing all analysis results
    """
    # Initialize results dictionary
    results = {}

    # 1. Get statements and create visualizations
    stmts_df, filtered_df = get_stmts_from_source(source_hgnc_id, target_proteins=target_hgnc_ids)

    # Create and convert interaction plot
    interaction_fig = plot_stmts_by_type(filtered_df)
    results['interaction_plot'] = convert_plot_to_base64(interaction_fig)

    results['statements'] = assemble_protein_stmt_htmls(filtered_df)

    # 2. Run discrete analysis
    hgnc_map = {hgnc_id: hgnc_client.get_hgnc_name(hgnc_id) for hgnc_id in target_hgnc_ids}
    discrete_result = discrete_analysis(hgnc_map, client=client)
    results['discrete_analysis'] = discrete_result

    # 3. Find shared pathways
    shared_pathways_result = shared_pathways_between_gene_sets([source_hgnc_id], target_hgnc_ids)
    results['shared_pathways'] = shared_pathways_result

    # 4. Analyze protein families
    shared_families_result = shared_protein_families(target_hgnc_ids, source_hgnc_id)
    results['protein_families'] = shared_families_result

    # 5. GO terms analysis
    source_go_terms, _ = get_go_terms_for_source(source_hgnc_id)
    shared_go_df = find_shared_go_terms(source_go_terms, target_hgnc_ids)
    results['go_terms'] = {
        'source_terms': source_go_terms,
        'shared_terms': shared_go_df
    }

    # 6. Additional analyses
    shared_proteins, shared_entities = shared_upstream_bioentities_from_targets(
        stmts_df,
        target_hgnc_ids
    )
    results['upstream'] = {
        'shared_proteins': shared_proteins,
        'shared_entities': shared_entities
    }

    # 7. Get combined pathway analysis
    pathways_df = combine_target_gene_pathways(source_hgnc_id, target_hgnc_ids)
    results['combined_pathways'] = pathways_df

    # 8. Create analysis plots
    if not shared_go_df.empty and not shared_entities.empty:
        # graph_boxplots now returns base64 string directly
        results['analysis_plot'] = graph_boxplots(shared_go_df, shared_entities)

    return results


@autoclient()
def explain_downstream(
    source: str,
    targets: List[str],
    *,
    client,
    id_type: str = 'hgnc.symbol'
) -> Dict:
    """High-level function that handles input validation and runs the analysis.

    Parameters
    ----------
    source : str
        Source identifier (either gene symbol or HGNC ID based on id_type)
    targets : List[str]
        List of target identifiers
    client :
        The client instance
    id_type : str
        Type of identifiers provided. Either 'hgnc.symbol' or 'hgnc'

    Returns
    -------
    Dict
        Complete analysis results
    """
    if id_type == 'hgnc.symbol':
        source_hgnc_id = get_valid_gene_id(source)
        target_hgnc_ids = get_valid_gene_ids(targets)

        if not source_hgnc_id:
            raise ValueError('Could not convert the source gene name to HGNC ID')
        if not target_hgnc_ids:
            raise ValueError('Could not convert any target gene names to HGNC IDs')

    elif id_type == 'hgnc':
        source_hgnc_id = source
        target_hgnc_ids = targets
    else:
        raise ValueError('Invalid id_type, must be hgnc.symbol or hgnc')

    # Run the main analysis with the validated IDs
    return run_explain_downstream_analysis(
        source_hgnc_id,
        target_hgnc_ids,
        client=client
    )
