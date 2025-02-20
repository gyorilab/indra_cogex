#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This module implements analysis of mechanisms connecting a source with a set of
downstream targets to construct possible explanations from the INDRA CoGEx
knowledge graph.

Possible explanations considered include INDRA statements, pathway membership,
determining if any of the proteins belong to the same protein family/complex
as the target, and using gene set enrichment on intermediates between
the source and the target.
"""
import itertools
import os
import json
import logging
from collections import defaultdict
import markupsafe

import pandas as pd
import matplotlib.pyplot as plt
from indra.assemblers.html import HtmlAssembler
from indra.statements import *
from indra.databases import hgnc_client
from indra.sources import indra_db_rest
from indra.tools import assemble_corpus as ac

from indra_cogex.client import *

logger = logging.getLogger(__name__)

from indra_cogex.analysis.gene_analysis import discrete_analysis


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
    """To get a dataframe of proteins that the source protien has direct INDRA
    relationship with to find the stmt_jsons, type, and id

    Parameters
    ----------
    source_id: string
        HGNC id for source protiens
    client: autoclient
    source_ns: str
        Ids entered are HGNC ids
    target_proteins: list
        Optional parameter to enter target proteins, default is none

    Returns
    -------
    stmts_by_protein_df: Dataframe
        Unfiltered dataframe that contains all INDRA relationships for
        source proteins
    stmts_by_protein_filtered_df: Dataframe
        Contains INDRA relationships for source proteins filtered by
        target_proteins
    """
    # Get indra_rel objects for proteins that have a direct INDRA relationship
    # with the source protein
    res = client.get_target_relations(
        source=(source_ns, source_id),
        relation='indra_rel',
        source_type='BioEntity',
        target_type='BioEntity',
        )
    
    # Extract necessary information from the result and creates dictionary
    records = [
           {
            "name": entry.target_name,
            "target_type": entry.target_ns,
            "target_id": entry.target_id,
            "stmt_type": entry.data["stmt_type"],
            "evidence_count":entry.data["evidence_count"],
            "stmt_hash":entry.data["stmt_hash"],
            "stmt_json": entry.data["stmt_json"]
             }
            for entry in res
        ]
    
    stmts_by_protein_df = pd.DataFrame.from_records(records)
    # If there are target proteins filters data frame based on that list
    if target_proteins:
        stmts_by_protein_filtered_df = stmts_by_protein_df[
            stmts_by_protein_df.target_id.isin(target_proteins)]
        # Adds evidences to the dataframe using stmt hashes
        evidences = []
        for stmt_hash in stmts_by_protein_filtered_df["stmt_hash"].values:
            evidences.append(get_evidences_for_stmt_hash(int(stmt_hash)))
        stmts_by_protein_filtered_df_copy = stmts_by_protein_filtered_df.copy()
        stmts_by_protein_filtered_df_copy["evidences"] = evidences
        logger.info("Dataframe of protiens that have INDRA relationships with HGNC:"+ source_id +
                    " that have been filtered:\n" + str(stmts_by_protein_filtered_df_copy))

    else:
        stmts_by_protein_filtered_df_copy = stmts_by_protein_df

    return stmts_by_protein_df, stmts_by_protein_filtered_df_copy


def plot_barchart(stmts_df, pathways_df,
                   interaction_fname, indra_rel_fname, pathways_fname):
    """Visualize frequency of interaction types among protiens that have direct
       INDRA relationship to source

    Parameters
    ----------
    stmts_df : pd.DataGrame
        Contains INDRA statements represented as a data frame.
    interaction_fname,indra_rel_fname,pathways_fname : str
        Names of the files bar chart will be saved into.
    """
    # Plot bar chart based on "stmt_type" which are the interaction types
    type_counts = stmts_df["stmt_type"].value_counts()
    type_counts.plot.bar()
    plt.xlabel("Interaction Type")
    plt.ylabel("Frequency")
    plt.title("Frequency of Type of Interaction With Source")
    plt.savefig(interaction_fname, bbox_inches="tight")

    type_counts = stmts_df["name"].value_counts()
    type_counts.plot.bar()
    plt.xlabel("Protein Name")
    plt.ylabel("# Indra Stmts")
    plt.title("Frequency of Indra Statements to Source")
    plt.savefig(indra_rel_fname, bbox_inches="tight")

    path_counts = pathways_df["target_protein"].value_counts()
    path_counts.plot.bar()
    plt.xlabel("Protein HGNC ID")
    plt.ylabel("# Shared Pathways")
    plt.title("Frequency of Shared Pathways with Source by Target")
    plt.savefig(pathways_fname, bbox_inches="tight")
    
    
def assemble_protein_stmt_htmls(stmts_df, output_path):
    """Assemble HTML page for each protein's INDRA statements in a data frame.

    Parameters
    ----------
    stmts_df : pd.DataFrame
        Contains INDRA relationships for source protein filtered by
        "target_proteins" genes
    output_path: str
        String for output path
    Returns
    -------
    stmt_html_list: list
        List of filenames for INDRA html pages
    """
    curs = indra_db_rest.get_curations()
    
    stmts_by_protein = defaultdict(list)
    for _, row in stmts_df.iterrows():
        stmt = stmt_from_json(json.loads(row['stmt_json']))
        stmt.evidence = row['evidences']
        stmts_by_protein[row['name']].append(stmt)

    stmt_html_list = []
    for name, stmts in stmts_by_protein.items():
        # Use HtmlAssembler to get html pages of INDRA statements for each gene
        stmts = ac.filter_by_curation(stmts, curs)
        if not stmts:
            continue
        ha = HtmlAssembler(stmts, title='Statements for %s' % name,
                           db_rest_url='https://db.indra.bio')
        fname = os.path.join(output_path, '%s_statements.html' % name)
        ha.save_model(fname)
        stmt_html_list.append(fname)

    return stmt_html_list

def shared_pathways_between_gene_sets(source_hgnc_ids, target_hgnc_ids):
    """Find shared pathways between list of target genes and source protein

    Parameters
    ----------
    target_hgnc_ids : list
        HGNC ids for a source set
    source_hgnc_ids : list
        HGNC ids for a target set

    Returns
    -------
    shared_pathways_list : pd.DataFrame
        Nested list of Relation objects describing the pathways shared for
        a given pair of genes.
    """
    shared_pathways_list = []

    for source_id, target_id in itertools.product(source_hgnc_ids, target_hgnc_ids):
        res = get_shared_pathways_for_genes((
            ("HGNC", target_id), ("HGNC", source_id)))
        if res:
            for entry in res:
                shared_pathways_list.append({"pathway_name": entry.data["name"],
                        "id": entry.db_id, "target_protein":"HGNC: " + target_id})

    if not shared_pathways_list:
        logger.info("There are no shared pathways between the "
                    "source and targets")
    shared_pathways_df = pd.DataFrame.from_records(shared_pathways_list)
    return shared_pathways_df

def shared_enriched_pathways(source_hgnc_id, discrete_result):
    """Find shared enriched pathways between gene sets

    Parameters
    ----------
    source_hgnc_id : string
        This is the source hgnc id.
    discrete_result : dict
        Dictionary of gene enrichment results

    Returns
    -------
    None.

    """
    reactome_df = discrete_result["reactome"]
    wikipathways_df = discrete_result["wikipathways"]
    shared_pathways_df = pd.concat([reactome_df, wikipathways_df])

    source_pathways = get_pathways_for_gene(("HGNC",source_hgnc_id))


    source_path_ids = [res.db_id for res in source_pathways]
    filtered_pathways = shared_pathways_df[shared_pathways_df.curie.isin(source_path_ids)]

    if filtered_pathways.empty:
        return None
    else:
        return filtered_pathways

@autoclient()
def shared_protein_families_between_gene_sets(target_hgnc_ids, source_hgnc_id, *, client):
    """ Determine if any gene in gene list isa/partof the source protein
    Parameters
    ----------
    target_hgnc_ids : list
        Contains HGNC ids for target list
    source_hgnc_id : str
        The source proteins HGNC ids

    Returns
    -------
    shared_families_df: pd.DataFrame
        Contains shared protein family complexes for the target proteins and
        the source
    """
    #  Iterates through source ids to format for query
    source_hgnc_id = "hgnc:"+source_hgnc_id
    # Iterates through target ids to format for query
    targets_list = [f"hgnc:{ids}" for ids in target_hgnc_ids]
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

        # if only the source or only the target returned results
        else:
            logger.info("There are no shared protein family complexes")
            return None

    # if the query didn't return results
    else:
        logger.info("There are no shared protein family complexes")
        return None


def get_go_terms_for_source(source_hgnc_id):
    """ This method gets the go terms for the source protein
    Parameters
    ----------
    source_hgnc_id: string
        HGNC ids strings for the source proteins

    Returns
    -------
    source_go_terms : list
        Contains the GO terms for source proteins
    """
    # these are the GO terms for target protein
    go_nodes = get_go_terms_for_gene(("HGNC", source_hgnc_id))
    source_go_terms = [
        go_node.db_id.lower() for go_node in go_nodes
    ]
    return source_go_terms
        

def shared_upstream_bioentities_from_targets(stmts_by_protein_df, discrete_result):
    """Use the indra_upstream csv to get a dataframe that is the
        intersection of the upstream molecules and the bioentities that target
        protein has direct INDRA relationships with and the bioentities that
        target protein has direct INDRA relationships with

    Parameters
    ----------
    stmts_by_protein_df: pd.DataFrame
        Contains all bioentities target protien has a direct INDRA relationship

    Returns
    -------
    shared_proteins : list
        list of shared bioentities between the indra_upstream results
        and bioenties that have direct INDRA relationships with target protein
    shared_entities: pd.DataFrame
        The filtered the indra_upstream_df using the shared_protiens list
        (can pick whether you want to filter the indra_upstream_df or
        protein_df which contains all bioentities that target protein has a
        direct INDRA relationship with)
    """
    # load csv into dataframe
    indra_upstream_df = discrete_result["indra-upstream"]

    # list that are shared entities between indra_upstream for gene set and
    # proteins that have a direct INDRA relationship with target protein
    shared_proteins = list((set(indra_upstream_df["name"].values)).intersection
                           (set(stmts_by_protein_df["name"].values)))

    if shared_proteins:
        shared_entities = indra_upstream_df[indra_upstream_df.name.
                                            isin(shared_proteins)]
        logger.info("These are the shared upstream bioentities between the"
                    "gene list and source_protein\n" + str(shared_entities))
        shared_entities.reset_index(inplace=True)
    # if there are no shared proteins
    else:
        shared_entities = None
        logger.info("There are no shared upstream bioentities between the "
                    "targets and the source")

    return shared_proteins, shared_entities


def shared_goterms_between_gene_sets(source_go_terms, discrete_result):
    """This method finds the shared go terms between the gene list and target
        proteins GO terms again the data is downloaded from the discrete gene
        analysis is as csv file

    Parameters
    ----------
    source_go_terms : list
        GO terms for the source proteins
    discrete_result : dict
        Dictionary of gene enrichment results

    Returns
    -------
    shared_df: pd.DataFrame
fstmt        Contains shared bioentities that have the same go terms
        between the GO terms provided from the gene analysis and GO terms
        associated with target protein
    """

    # loads data fron csv file
    go_terms_df = discrete_result["go"]

    # gets list of shared go terms between protein list and target protien
    shared_go = list((set(go_terms_df["curie"].values).
                      intersection(set(source_go_terms))))
    if shared_go:
        # filters the go terms dataframe by the id of the protiens in shared_go
        shared_go_df = go_terms_df[go_terms_df.curie.isin(shared_go)]
        logger.info("These are shared complexes between the gene list and the "
                    "source_protein\n" + str(shared_go_df))

    else:
        logger.info("There are no shared go terms between the source and targets")
        return None

    return shared_go_df


def combine_target_gene_pathways(reactome_filename, wiki_filename):
    """This method creates combined dataframe of REACTOME and Wikipathways
    provided by gene analysis for gene list

    Parameters
    ----------
    reactome_filename : str
        The file path to the CSV file containing the REACTOME pathways data.
    wiki_filename : str
        The file path to the CSV file containing the WikiPathways data.

    Returns
    -------
    pathways_df : dataframe
        This dataframe contains the combined wikipathways and reactome
        pathways for the gene list
    """
    reactome_df = pd.read_csv(reactome_filename)
    wikipathways_df = pd.read_csv(wiki_filename)
    pathways_df = pd.concat([reactome_df, wikipathways_df])

    return pathways_df


def graph_boxplots(shared_go_df,shared_entities, filename):
    """ This method creates boxplots to visualize p and q values for
        shared complexes/GO terms and bioentities

    Parameters
    ----------
    shared_go_df : pd.DataFrame
        Contains shared bioentities that have the same go terms
        between the GO terms provided from the gene analysis and GO terms
        associated with source protein.

    shared_entities : pd.DataFrame
        The filtered the indra_upstream_df using the shared_proteins list
        (you can pick whether you want to filter the indra_upstream_df or
        protein_df which contains all bioentities that source protein has a
        direct INDRA relationship with).
    filename: str
        name of the file chart will be downloaded under
    """
    # plots boxplots for each type of graph
    fig, axs = plt.subplots(2, 2, figsize=(12, 8))

    if shared_go_df is not None:
        axs[0, 0].set_title("P-values for Shared Go Terms")
        shared_go_df.boxplot(column=["p"], ax=axs[0, 0])

        axs[0, 1].set_title("Q-values for Shared Go Terms")
        shared_go_df.boxplot(column=["q"], ax=axs[0, 1])
    else:
        axs[0, 0].set_title("No Shared Go Terms")
        axs[0, 1].set_title("No Shared Go Terms")

    if shared_entities is not None:
        axs[1, 0].set_title("P-values for Shared Bioentities")
        shared_entities.boxplot(column=["p"], ax=axs[1, 0])

        axs[1, 1].set_title("Q-values for Shared Bioentities")
        shared_entities.boxplot(column=["q"], ax=axs[1, 1])
        plt.savefig(filename, bbox_inches="tight")
    else:
        axs[0, 0].set_title("No Shared Go Bioentities")
        axs[0, 1].set_title("No Shared Go Bioentities")


@autoclient()
def run_explain_downstream_analysis(source_hgnc_id, target_hgnc_ids, output_path, *, client):
    """This method uses the HGNC ids of the source and targets
        to pass into and call other methods

    Parameters
    ----------
    output_path : str
        Path where output files such as visualizations and CSVs will be saved.
    client : Neo4jClient
        The client object used to handle database connections or API interactions.
    source_hgnc_id : str
        The HGNC id for the source protein
    target_hgnc_ids : list
        List of strings of HGNC ids for target proteins
    Returns
    -------
    result: dict
        Dictionary of analysis results
    """
    # Get filtered dataframe by protiens that source has INDRA rel with
    stmts_by_protein_df, stmts_by_protein_filtered_df = \
        get_stmts_from_source(source_hgnc_id, target_proteins=target_hgnc_ids)
   
    # Get INDRA statements for protiens that have direct INDRA rel
    stmts_html_list = assemble_protein_stmt_htmls(stmts_by_protein_filtered_df, output_path)
    
    discrete_result = discrete_analysis(
        target_hgnc_ids, client=client, indra_path_analysis=True
    )
    for k, v in discrete_result.items():
        # The values here are data frames
        v.to_csv(os.path.join(output_path, f"{k}_discrete.csv"))
    # Find shared pathways between users gene list and target protein
    
    shared_pathways_df = shared_pathways_between_gene_sets([source_hgnc_id],
                                                               target_hgnc_ids)
    shared_pathways_result = shared_enriched_pathways(source_hgnc_id, discrete_result)

    with open(os.path.join(output_path, "shared_pathways.txt"), "w") as fh:
        fh.write(str(shared_pathways_result))

    # Visualize frequnecy of interaction types among protiens that have direct
    # INDRA relationship to source
    interaction_fname = os.path.join(output_path,
                                              "interaction_barchart.png")

    indra_rel_fname = os.path.join(output_path,
                                              "indra_relationship_frequency.png")

    pathways_fname = os.path.join(output_path,
                                              "pathways_frequency.png")
    plot_barchart(stmts_by_protein_filtered_df, shared_pathways_df,
                       interaction_fname, indra_rel_fname, pathways_fname)

    # Determine which proteins of interest are part of the same protien\
    # family/complex as the target
    shared_families_result = shared_protein_families_between_gene_sets(target_hgnc_ids, source_hgnc_id)
   
    with open(os.path.join(output_path, "shared_families.txt"), "w") as fh:
        fh.write(str(shared_families_result))

    # Get go term ids for target gene
    source_go_terms = get_go_terms_for_source(source_hgnc_id)

    shared_proteins, shared_entities = \
        shared_upstream_bioentities_from_targets(stmts_by_protein_df,
                                                 discrete_result)

    # Get shared bioentities between target list and source protein using GO terms
    shared_go_df = shared_goterms_between_gene_sets(source_go_terms, discrete_result)
    print(shared_go_df)

    # Visualizes p and q values for shared GO terms
    go_graph_fname = os.path.join(output_path, 'shared_go_terms.png')
    graph_boxplots(shared_go_df, shared_entities, go_graph_fname)
    
    stmt_df_html = stmts_by_protein_filtered_df.to_html(classes='table table-striped table-sm')
    if shared_families_result is not None:
        shared_family_html = shared_families_result.to_html(classes='table table-striped table-sm')
    else:
        shared_family_html = "There are no shared families"
    if shared_entities is not None:
        upstream_entities_html = shared_entities.to_html(classes='table table-striped table-sm')
    else:
        upstream_entities_html = "There are no shared upstream entities"
    if shared_pathways_result is not None:
        shared_pathways_html = shared_pathways_result.to_html(classes='table table-striped table-sm')
    else:
        shared_pathways_html = "There are no shared pathways"
    if shared_go_df is not None:
        shared_go_html = shared_go_df.to_html(classes='table table-striped table-sm')
    else:
        shared_go_html = "There are no shared go terms between gene sets"
    
    stmt_content = []
    for stmt_html_filename in stmts_html_list:
        with open(stmt_html_filename, 'r') as f:
            stmt_html_content = f.read()
            stmt_content.append(stmt_html_content)

    result = {"staments_by_protein_filtered":stmt_df_html,
              "interaction_chart":interaction_fname, 
              "indra_rel_chart": indra_rel_fname, 
              "pathways_chart":pathways_fname,
              "shared_families":shared_family_html,
              "upstream_entities":upstream_entities_html,
              "shared_pathways": shared_pathways_html,
              "shared_go_terms": shared_go_html,
              "stats_boxplot": go_graph_fname,
              "indra_stmt_html_contents": [markupsafe.Markup(html) for html in stmt_content]
              }
    
    return result

@autoclient()
def explain_downstream(source, targets, output_path, *, client, id_type='hgnc.symbol'):
    if id_type == 'hgnc.symbol':
        target_hgnc_ids = get_valid_gene_ids(targets)
        source_hgnc_id = get_valid_gene_id(source)

        if not source_hgnc_id:
            raise ValueError('Could not convert the source gene name to '
                             'HGNC ID, aborting.')

        # Remove the source from the targets in case it is there
        target_hgnc_ids = [hgnc_id for hgnc_id in target_hgnc_ids
                           if hgnc_id != source_hgnc_id]

        if not target_hgnc_ids:
            raise ValueError('Could not convert any target gene names to '
                             'HGNC IDs, aborting.')


    elif id_type == 'hgnc':
        source_hgnc_id = source
        target_hgnc_ids = targets
    else:
        raise ValueError('Invalid id_type, must be hgnc.symbol or hgnc.')

    if not os.path.exists(output_path):
        logger.info(f"Creating output directory {output_path}")
        os.makedirs(output_path)
    
    return run_explain_downstream_analysis(source_hgnc_id, target_hgnc_ids, output_path,
                                           client=client)
