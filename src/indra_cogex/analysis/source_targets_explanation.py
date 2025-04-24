"""
This module implements analysis of mechanisms connecting a source with a set of
downstream targets to construct possible explanations from the INDRA CoGEx
knowledge graph.

Possible explanations considered include INDRA statements, pathway membership,
determining if any of the proteins belong to the same protein family/complex
as the target, and using gene set enrichment on intermediates between
the source and the target.
"""
import json
import base64
import logging
import itertools
import os
import pickle
from collections import defaultdict
from typing import List, Tuple, Optional, Dict, Any
from io import BytesIO
from bs4 import BeautifulSoup

import pandas as pd
import matplotlib

from indra_cogex.apps.utils import format_stmts

matplotlib.use('agg')
import matplotlib.pyplot as plt

from indra.databases import hgnc_client
from indra.assemblers.html import HtmlAssembler
from indra.statements import *

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
    source_ns : str, optional
        The namespace for the source protein identifier (default: 'HGNC')
    client :
        The client instance
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
            "stmt_hash": entry.data["stmt_hash"],
            "source_counts": json.loads(entry.data["source_counts"]),
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
    """Assemble statement data for each protein's INDRA statements in a data frame.

    This function transforms statement data into a format that can be used by
    Vue.js components on the frontend and by REST API endpoints.

    Parameters
    ----------
    stmts_df : pd.DataFrame
        Contains INDRA relationships for source protein filtered by
        "target_proteins" genes

    Returns
    -------
    :
        Dictionary mapping protein names to formatted statement data
    """
    # Group statements by protein (gene) name
    stmt_data_per_gene = {}
    for name, gene_stmts_df in stmts_df.groupby('name'):
        # Convert statement JSON to statement objects
        stmts = stmts_from_json(
            [json.loads(sj) for sj in gene_stmts_df["stmt_json"].values]
        )

        # Get evidence counts for each statement hash
        evidence_counts = {
            int(sh): int(sc) for sh, sc in gene_stmts_df[["stmt_hash", "evidence_count"]].values
        }

        # Get source counts for each statement hash
        source_counts_per_hash = {
            int(sh): json.loads(sc) for sh, sc in gene_stmts_df[["stmt_hash", "source_counts"]].values
        }

        # Format statements for Vue.js rendering
        stmt_data_per_gene[name] = format_stmts(
            stmts,
            evidence_counts=evidence_counts,
            remove_medscan=True,  # fixme: provide from outside based on login
            source_counts_per_hash=source_counts_per_hash,
        )

    return stmt_data_per_gene


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
    return pd.DataFrame()


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
    q_value_threshold: float = 0.05,
    max_regulators: int = 50,
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
    client : Neo4jClient
        The client instance
    q_value_threshold : float, optional
        Maximum q-value for statistical significance filtering, default=0.05
    max_regulators : int, optional
        Maximum number of regulators to return, ordered by significance, default=50

    Returns
    -------
    :
        Shared proteins and detailed analysis results
    """
    # Get upstream analysis from database
    upstream_df = indra_upstream_ora(
        client=client,
        gene_ids=target_genes
    )

    # Ensure upstream_df has proper CURIE format
    if 'curie' in upstream_df.columns:
        # Check if CURIEs have namespace prefix (like "hgnc:")
        has_prefix = upstream_df['curie'].str.contains(':', na=False)

        # For those without prefix, add appropriate namespace
        if not all(has_prefix):
            # Add HGNC namespace to numeric IDs that don't have a prefix
            numeric_without_prefix = (~has_prefix) & upstream_df['curie'].str.match(r'^\d+$', na=False)
            upstream_df.loc[numeric_without_prefix, 'curie'] = 'hgnc:' + upstream_df.loc[
                numeric_without_prefix, 'curie']

    # Find shared proteins
    shared_proteins = list(set(upstream_df["name"].values).intersection(
        set(stmts_by_protein_df["name"].values)))

    if shared_proteins:
        # Get the entities data for these proteins
        shared_entities = upstream_df[upstream_df.name.isin(shared_proteins)]

        # Apply statistical filtering (assuming 'q' column always exists)
        significant_entities = shared_entities[shared_entities.q <= q_value_threshold]

        # Sort by significance (assuming 'q' column always exists)
        sorted_entities = significant_entities.sort_values('q')

        # Limit to top N regulators
        limited_entities = sorted_entities.head(max_regulators)
        limited_proteins = limited_entities.name.tolist()

        return limited_proteins, limited_entities
    else:
        return [], pd.DataFrame()


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
    :
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
    return pd.DataFrame(columns=go_df.columns)


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
    :
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
    :
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

    # Use the helper function to convert the figure to base64
    return convert_plot_to_base64(fig)


def convert_plot_to_base64(fig):
    """Helper function to convert matplotlib figure to base64 string."""
    # Save directly to BytesIO buffer
    buffer = BytesIO()
    fig.savefig(buffer, format='png', bbox_inches='tight')
    plt.close(fig)

    # Get base64 string
    buffer.seek(0)
    plot_data = base64.b64encode(buffer.read()).decode('utf-8')
    buffer.close()

    return plot_data


@autoclient()
def run_explain_downstream_analysis(source_hgnc_id, target_hgnc_ids, output_dir=None, *, client):
    """Run complete downstream analysis and save results to files.

    Parameters
    ----------
    source_hgnc_id : str
        HGNC ID for the source gene
    target_hgnc_ids : list
        List of HGNC IDs for target genes
    output_dir : str, optional
        Directory to save output files. If None, results are only returned as dict
    client :
        The client instance

    Returns
    -------
    :
        Dictionary containing all analysis results
    """
    # Initialize results dictionary
    results = {}

    # Create output directory if specified and it doesn't exist
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 1. Get statements and create visualizations
    stmts_df, filtered_df = get_stmts_from_source(source_hgnc_id, target_proteins=target_hgnc_ids)

    # Create and convert interaction plot
    interaction_fig = plot_stmts_by_type(filtered_df)
    results['interaction_plot'] = convert_plot_to_base64(interaction_fig)

    # Save interaction plot if output directory is specified
    if output_dir:
        # Save as PNG image
        interaction_fig.savefig(os.path.join(output_dir, 'interaction_plot.png'))
        # Also save the base64 data for convenience
        with open(os.path.join(output_dir, 'interaction_plot.txt'), 'w') as f:
            f.write(results['interaction_plot'])

    # Process statements - Using the new Vue-compatible approach
    # This transforms the statement information into a format that can be used to
    # render statements using the Vue.js components on the frontend
    stmt_data_per_gene = {}
    for name, gene_stmts_df in filtered_df.groupby('name'):
        stmts = stmts_from_json(
            [json.loads(sj) for sj in gene_stmts_df["stmt_json"].values]
        )
        evidence_counts = {
            int(sh): int(sc) for sh, sc in gene_stmts_df[["stmt_hash", "evidence_count"]].values
        }
        source_counts_per_hash = {
            int(sh): json.loads(sc) for sh, sc in gene_stmts_df[["stmt_hash", "source_counts"]].values
        }
        stmt_data_per_gene[name] = format_stmts(
            stmts,
            evidence_counts=evidence_counts,
            remove_medscan=True,  # fixme: provide from outside based on login
            source_counts_per_hash=source_counts_per_hash,
        )

    results['statements'] = stmt_data_per_gene

    if output_dir:
        # Create statements directory
        statements_dir = os.path.join(output_dir, 'statements')
        os.makedirs(statements_dir, exist_ok=True)

        # Save the complete statements data
        with open(os.path.join(statements_dir, 'all_statements.json'), 'w') as f:
            json.dump(stmt_data_per_gene, f, default=str, indent=2)

        # Also save individual gene statements separately for easier access
        for gene_name, stmt_data in stmt_data_per_gene.items():
            gene_file = os.path.join(statements_dir, f'{gene_name}_statements.json')
            with open(gene_file, 'w') as f:
                json.dump(stmt_data, f, default=str, indent=2)

    # 2. Run discrete analysis
    hgnc_map = {hgnc_id: hgnc_client.get_hgnc_name(hgnc_id) for hgnc_id in target_hgnc_ids}
    discrete_result = discrete_analysis(hgnc_map, client=client)
    results['discrete_analysis'] = discrete_result
    if output_dir:
        with open(os.path.join(output_dir, 'discrete_analysis.json'), 'w') as f:
            json.dump(discrete_result, f, default=str, indent=2)

    # 3. Find shared pathways
    shared_pathways_result = shared_pathways_between_gene_sets([source_hgnc_id], target_hgnc_ids)
    results['shared_pathways'] = shared_pathways_result
    if output_dir:
        with open(os.path.join(output_dir, 'shared_pathways.json'), 'w') as f:
            json.dump(shared_pathways_result, f, default=str, indent=2)

    # 4. Analyze protein families
    shared_families_result = shared_protein_families(target_hgnc_ids, source_hgnc_id)
    results['protein_families'] = shared_families_result
    if output_dir:
        with open(os.path.join(output_dir, 'protein_families.json'), 'w') as f:
            json.dump(shared_families_result, f, default=str, indent=2)

    # 5. GO terms analysis
    source_go_terms, _ = get_go_terms_for_source(source_hgnc_id)
    shared_go_df = find_shared_go_terms(source_go_terms, target_hgnc_ids)
    results['go_terms'] = {
        'source_terms': source_go_terms,
        'shared_terms': shared_go_df
    }
    if output_dir:
        go_terms_dir = os.path.join(output_dir, 'go_terms')
        os.makedirs(go_terms_dir, exist_ok=True)

        # Save source terms
        with open(os.path.join(go_terms_dir, 'source_terms.json'), 'w') as f:
            json.dump(source_go_terms, f, default=str, indent=2)

        # Save shared terms dataframe
        if not shared_go_df.empty:
            shared_go_df.to_csv(os.path.join(go_terms_dir, 'shared_terms.csv'))
            # Also save as HTML for easy viewing
            shared_go_df.to_html(os.path.join(go_terms_dir, 'shared_terms.html'))

    # 6. Additional analyses
    shared_proteins, shared_entities = shared_upstream_bioentities_from_targets(
        stmts_df,
        target_hgnc_ids
    )
    results['upstream'] = {
        'shared_proteins': shared_proteins,
        'shared_entities': shared_entities
    }
    if output_dir:
        upstream_dir = os.path.join(output_dir, 'upstream')
        os.makedirs(upstream_dir, exist_ok=True)
        with open(os.path.join(upstream_dir, 'shared_proteins.json'), 'w') as f:
            json.dump(shared_proteins, f, default=str, indent=2)
        with open(os.path.join(upstream_dir, 'shared_entities.json'), 'w') as f:
            json.dump(shared_entities, f, default=str, indent=2)

    # 7. Get combined pathway analysis
    pathways_df = combine_target_gene_pathways(source_hgnc_id, target_hgnc_ids)
    results['combined_pathways'] = pathways_df
    if output_dir and not pathways_df.empty:
        pathways_df.to_csv(os.path.join(output_dir, 'combined_pathways.csv'))
        # Also save as HTML for easy viewing
        pathways_df.to_html(os.path.join(output_dir, 'combined_pathways.html'))

    # 8. Create analysis plots
    if not shared_go_df.empty and not shared_entities.empty:
        # graph_boxplots now returns base64 string directly
        results['analysis_plot'] = graph_boxplots(shared_go_df, shared_entities)
        if output_dir:
            # Save as base64 text file
            with open(os.path.join(output_dir, 'analysis_plot.txt'), 'w') as f:
                f.write(results['analysis_plot'])

            # Also try to convert and save as PNG if possible
            try:
                import base64
                from io import BytesIO
                from PIL import Image

                # Assuming the base64 string is for an image
                img_data = base64.b64decode(results['analysis_plot'].split(',')[1])
                img = Image.open(BytesIO(img_data))
                img.save(os.path.join(output_dir, 'analysis_plot.png'))
            except Exception:
                # If conversion fails, just skip the PNG output
                pass

    # Optionally, still save the complete results as a pickle for backwards compatibility
    if output_dir:
        with open(os.path.join(output_dir, 'analysis_results.pkl'), 'wb') as f:
            pickle.dump(results, f)

    return results


@autoclient()
def source_target_analysis(
    source: str,
    targets: List[str],
    output_dir: Optional[str] = None,
    *,
    client,
    id_type: str = 'hgnc.symbol'
) -> Dict:
    """High-level function that handles input validation and runs the analysis.

    Corresponding web-form based analysis can be found at:
    https://discovery.indra.bio/source_target/analysis

    Parameters
    ----------
    source : str
        Source identifier (either gene symbol or HGNC ID based on id_type)
    targets : List[str]
        List of target identifiers
    output_dir : str, optional
        Directory to save output files. If None, results are only returned as dict
    client :
        The client instance
    id_type : str
        Type of identifiers provided. Either 'hgnc.symbol' or 'hgnc'. Use
        'hgnc.symbol' if the source and targets fields are HGNC gene symbols, and
        'hgnc' if they are HGNC IDs.

    Returns
    -------
    :
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

    # Run the main analysis with the validated IDs and output directory
    return run_explain_downstream_analysis(
        source_hgnc_id,
        target_hgnc_ids,
        output_dir=output_dir,
        client=client
    )
