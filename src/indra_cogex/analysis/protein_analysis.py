#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Protein Analysis Exploration

Exploring how a set of target protiens relate to a source protein through
INDRA statements, exploring pathway membership, determining if any of the
proteins belong to the same protein family/complex as the target and using
INDRA discrete gene list analysis results
"""
import itertools
import os
import json
import logging
from collections import defaultdict

import pandas as pd
import matplotlib.pyplot as plt
from indra.assemblers.html import HtmlAssembler
from indra.statements import *
from indra.databases import hgnc_client

from indra_cogex.client import *

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
    source_protein: string
        The protein of interest in relation to protien list user enters

    target_proteins: list
        Contains proteins user enters to analyze in relation to target

    Returns
    -------
    stmts_by_protein_df: Dataframe
        Unfiltered dataframe that contains all INDRA relationships for
        target protein
    stmts_by_protein_filtered_df: dataframe
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


    # Extract necessary information from the result and creates dictionary
    # TODO: couldn't this be implemented using a list of dicts with
    # a single dict-comprehension that is then loadded into a data frame?
    jsons = []
    types = []
    ids = []
    stmt_types = []
    names = []
    for entry in res:
        names.append(entry.target_name)
        jsons.append(entry.data["stmt_json"])
        types.append(entry.target_ns)
        ids.append(entry.target_id)
        stmt_types.append(entry.data["stmt_type"])
    protein_dict = {"name": names, "stmt_json": jsons, "target_type": types,
                    "target_id": ids, "stmt_type": stmt_types}
    stmts_by_protein_df = pd.DataFrame(protein_dict)

    # If there are target proteins filters data frame based on that list
    if target_proteins:
        stmts_by_protein_filtered_df = stmts_by_protein_df[
            stmts_by_protein_df.target_id.isin(target_proteins)]
        logger.info("Dataframe of protiens that have INDRA relationships with source\
                    that have been filtered:\n" + str(stmts_by_protein_filtered_df))

    else:
        stmts_by_protein_filtered_df = stmts_by_protein_df

    return stmts_by_protein_df, stmts_by_protein_filtered_df


def plot_stmts_by_type(stmts_df, fname):
    """Visualize frequnecy of interaction types among protiens that have direct
       INDRA relationship to source

    Parameters
    ----------
    stmts_df : pd.DataGrame
        Contains INDRA statements represented as a data frame.
    fname : str
        Name of the file bar chart will be saved into.
    """
    # Plot bar chart based on "stmt_type" which are the interaction types
    type_counts = stmts_df["stmt_type"].value_counts()
    type_counts.plot.bar()
    plt.xlabel("Interaction Type")
    plt.ylabel("Frequency")
    plt.title("Frequency of Type of Interaction With Target")

    plt.savefig(fname, bbox_inches="tight")


def assemble_protein_stmt_htmls(stmts_df, output_path):
    """Assemble HTML page for each protein's INDRA statements in a data frame.

    Parameters
    ----------
    stmts_df : pd.DataFrame
        Contains INDRA relationships for source protein filtered by
        "target_proteins" genes
    """
    # FIXME: the fact that there are multiple files generated for a given
    # protein indicates that the data frame is not grouping statements
    # as expected, and there are multiple rows for each protein name
    stmts_by_protein = defaultdict(list)
    for _, row in stmts_df.iterrows():
        stmts = stmts_from_json(json.loads(row['stmt_json']))
        stmts_by_protein[row['name']] += stmts

    for name, stmts in stmts_by_protein.items():
        # uses HtmlAssembler to get html pages of INDRA statements for each gene
        ha = HtmlAssembler(stmts, title='Statements for %s' % name,
                           db_rest_url='https://db.indra.bio')
        # FIXME: why do we need the index here?
        fname = os.path.join(output_path, '%s_statements.html' % name)
        ha.save_model('%s_statements.html' % fname)


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
    # FIXME: is there a reason to use a list here instead of a set?
    # this  presumably results in the same pathway being listed multiple times
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
    """ Determine if any gene in gene list isa/partof the source protein
    Parameters
    ----------
    target_hgnc_ids : list
        Contains HGNC ids for target list
    source_hgnc_id : string
        The source proteins HGNC id

    Returns
    -------
    shared_families_df: dataframe
        Contains shared protein family complexes for the target proteins and
        the source
    """
    # adds hgnc: to the beginning of source id to format for query
    source_hgnc_id = "hgnc:"+source_hgnc_id

    # iterates through target ids to format for query
    targets_list = []
    for ids in target_hgnc_ids:
        target_id = "hgnc:"+ids
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
        HGNC id for the source protein

    Returns
    -------
    target_go: list
        Contains the GO terms for target proteins
        FIXME: documentation seems to be wrong here
    go_nodes: list
        List of node objects that has information about GO terms for t
        arget protein
    """
    # these are the GO terms for target protein
    go_nodes = get_go_terms_for_gene(("HGNC", source_hgnc_id))
    source_go_terms = [
        go_node.db_id.lower() for go_node in go_nodes
    ]

    return source_go_terms, go_nodes


def shared_upstream_bioentities_from_targets(stmts_by_protein_df, filename):
    """This method uses the indra_upstream csv to get a dataframe that is the
        intersection of the upstream molecules and the bioentities that target
        protein has direct INDRA relationships with and the bioentities that
        target protein has direct INDRA relationships with

    Parameters
    ----------
    stmts_by_protein_df: dataframe
        Contains all bioentities target protien has a direct INDRA relationship

    Returns
    -------
    shared_proteins: list
        list of shared bioentities between the indra_upstream results
        and bioenties that have direct INDRA relationships with target protein

    shared_entities: dataframe
        The filtered the indra_upstream_df using the shared_protiens list
        (can pick whether you want to filter the indra_upstream_df or
        protein_df which contains all bioentities that target protein has a
        direct INDRA relationship with)
    """
    # load csv into dataframe
    indra_upstream_df = pd.read_csv(filename)

    # list that are shared entities between indra_upstream for gene set and
    # proteins that have a direct INDRA relationship with target protein
    shared_proteins = list((set(indra_upstream_df["Name"].values)).intersection
                           (set(stmts_by_protein_df["name"].values)))

    if shared_proteins:
        shared_entities = indra_upstream_df[indra_upstream_df.Name.
                                            isin(shared_proteins)]
        logger.info("These are the shared upstream bioentities between the"
                    "gene list and source_protein\n" + str(shared_entities))

    # if there are no shared proteins
    else:
        logger.info("There are no shared upstream bioentities between the "
                    "targets and the source")

    return shared_proteins, shared_entities


def find_shared_go_terms(source_go_terms, filename):
    """This method finds the shared go terms between the gene list and target
        proteins GO terms again the data is downloaded from the discrete gene
        analysis is as csv file

    Parameters
    ----------
    source_go_terms: list
        GO terms for the source proteins

    Returns
    -------
    shared_df: dataframe
        Contains shared bioentities that have the same go terms
        between the GO terms provided from the gene analysis and GO terms
        associated with target protein
    """

    # loads data fron csv file
    go_terms_df = pd.read_csv(filename)

    # gets list of shared go terms between protein list and target protien
    shared_go = list((set(go_terms_df["CURIE"].values).
                      intersection(set(source_go_terms))))
    if shared_go:
        # filters the go terms dataframe by the id of the protiens in shared_go
        shared_go_df = go_terms_df[go_terms_df.CURIE.isin(shared_go)]
        logger.info("These are shared complexes between the gene list and the",
                    "source_protein\n" + str(shared_go_df))

    else:
        logger.info("There are no shared go terms between the source and targets")
        return None

    return shared_go_df


def combine_target_gene_pathways(reactome_filename, wiki_filename):
    """ This method creates combined dataframe of REACTOME and Wikipathways
    provided by gene analysis for gene list

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
        shared complexes/GO terms and bioentiies


    Parameters
    ----------
    shared_complexes_df : dataframe
        Contains shared bioentities that have the same go terms
        between the GO terms provided from the gene analysis and GO terms
        associated with source protein.

    shared_entities : dataframe
        The filtered the indra_upstream_df using the shared_protiens list
        (you can pick whether you want to filter the indra_upstream_df or
        protein_df which contains all bioentities that source protein has a
        direct INDRA relationship with).

    filename: string
        name of the file chart will be downloaded under
    """

    # plots boxplots for each type of graph
    fig, axs = plt.subplots(2, 2, figsize=(12, 8))

    axs[0, 0].set_title("P-values for Shared Go Terms")
    shared_go_df.boxplot(column=["p-value"], ax=axs[0, 0])

    axs[0, 1].set_title("Q-values for Shared Go Terms")
    shared_go_df.boxplot(column=["q-value"], ax=axs[0, 1])

    axs[1, 0].set_title("P-values for Shared Bioentities")
    shared_entities.boxplot(column=["p-value"], ax=axs[1, 0])

    axs[1, 1].set_title("Q-values for Shared Bioentities")
    shared_entities.boxplot(column=["q-value"], ax=axs[1, 1])
    plt.savefig(filename, bbox_inches="tight")


def run_explain_downstream_analysis(source_hgnc_id, target_hgnc_ids, output_path):
    """This method uses the HGNC ids of the source and targets
        to pass into and call other methods

    Parameters
    ----------
    source_hgnc_id : string
        The HGNC id for the source protein
    target_hgnc_ids : list
        List of strings of HGNC ids for target proteins
    """
    # Get filtered dataframe by protiens that source has INDRA rel with
    stmts_by_protein_df, stmts_by_protein_filtered_df = \
        get_stmts_from_source(source_hgnc_id, target_proteins=target_hgnc_ids)

    # Visualize frequnecy of interaction types among protiens that have direct
    # INDRA relationship to source
    interaction_barchart_fname = os.path.join(output_path,
                                              "interaction_barchart.png")
    plot_stmts_by_type(stmts_by_protein_filtered_df,
                       interaction_barchart_fname)

    # Get INDRA statements for protiens that have direct INDRA rel
    assemble_protein_stmt_htmls(stmts_by_protein_filtered_df, output_path)

    # Find shared pathways between users gene list and target protein
    shared_pathways_result = shared_pathways_between_gene_sets([source_hgnc_id],
                                                               target_hgnc_ids)
    # FIXME: Is a plain text file the right choice here?
    with open(os.path.join(output_path, "shared_pathways.txt"), "w") as fh:
        fh.write(str(shared_pathways_result))

    # Determine which proteins of interest are part of the same protien\
    # family/complex as the target
    shared_families_result = shared_protein_families(target_hgnc_ids, source_hgnc_id)
    # FIXME: Is a plain text file the right choice here?
    with open(os.path.join(output_path, "shared_families.txt"), "w") as fh:
        fh.write(str(shared_families_result))

    # Get go term ids for target gene
    source_go_terms, go_nodes = get_go_terms_for_source(source_hgnc_id)

    # Find shared upstream bioentities between the target list and source protein
    upstream_fname = os.path.join(output_path, "shared_upstream.csv")
    shared_proteins, shared_entities = \
        shared_upstream_bioentities_from_targets(stmts_by_protein_df,
                                                 upstream_fname)

    # Get shared bioentities between target list and source protein using GO terms
    go_fname = os.path.join(output_path, "shared_go_terms.csv")
    shared_go_df = find_shared_go_terms(source_go_terms, go_fname)

    # Get a data frame of reactome and wikipathways for shared genes
    reactome_fname = os.path.join(output_path, "shared_reactome.csv")
    wiki_fname = os.path.join(output_path, "shared_wikipathways.csv")
    pathways_df = combine_target_gene_pathways(reactome_fname, wiki_fname)

    # Visualizes p and q values for shared GO terms
    go_graph_fname = os.path.join(output_path, 'shared_go_terms.png')
    graph_boxplots(shared_go_df, shared_entities, go_graph_fname)


def explain_downstream(source, targets, output_path, id_type='hgnc.symbol'):
    if id_type == 'hgnc.symbol':
        source_hgnc_id = get_valid_gene_id(source)
        target_hgnc_ids = get_valid_gene_ids(targets)

        if not source_hgnc_id:
            raise ValueError('Could not convert the source gene name to '
                             'HGNC ID, aborting.')

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

    return run_explain_downstream_analysis(source_hgnc_id, target_hgnc_ids, output_path)
