#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Protein Analysis Exploration

Exploring how a unique set of protiens relates to a target protein through 
INDRA statements, exploring pathway membership,determining if any of the proteins 
belong to the same protein family/complex as the target and using 
INDRA discrete gene list analysis results
"""

import json

import pandas as pd
import matplotlib.pyplot as plt
from indra.assemblers.html import HtmlAssembler
from indra.statements import *
from indra.databases import hgnc_client

from indra_cogex.client import *


def get_stmts_from_source(source_protein, target_proteins=None):
    """To get a dataframe of proteins that the target protien has direct INDRA
    relationship with to find the stmt_jsons, type, and id
    
    Parameters
    ----------
    source_protein: string
        The protein of interest in relation to protien list user enters
    
    protein_list: list 
        Contains proteins user enters to analyze in relation to target
        
    Returns
    -------
    filtered_df: dataframe 
        Contains INDRA relationships for target protein filtered by "protein_list" genes
    protein_df: Dataframe
        Unfiltered dataframe that contains all INDRA relationships for target protein  
    """
    res = client.get_target_relations(
        source=('HGNC', source_protein),
        relation='indra_rel',
        source_type='BioEntity',
        target_type='BioEntity',
    )

    # TODO: get the same values from this result as what you got from the old
    # query

    #cypher to get dataframe with all proteins that have INDRA relationship with target protein
    #query = f"""MATCH p=(n:BioEntity)-[r:indra_rel]->(m:BioEntity) WHERE n.name = '{source_protein}' RETURN m.name, r.stmt_json, m.type, m.id, r.stmt_type"""
    #res = client.query_tx(query)
    
    
    jsons = []
    types = []
    ids = []
    stmt_types = []
    for i in range(len(res)):
        target_name = res[i].data
        jsons.append(res[i].data["stmt_json"])
        types.append(res[i].target_ns)
        ids.append(res[i].target_id)
        stmt_types.append(res[i].data["stmt_type"])
    protein_dict = {"stmt_json": jsons, "target_type": types, "target_id":ids, "stmt_type": stmt_types}
    stmts_by_protein_df = pd.DataFrame(protein_dict)
    
    print(stmts_by_protein_df)
    print(res[0].__dict__)
   
    stmts_by_protein_df = pd.DataFrame(res, columns=["name", "stmt_json", "type", "id", "indra_type"])
    if target_proteins:
        # TODO: since the target proteins are now HGNC ids, you need to change this filter
        # to be using HGNC ids
        stmts_by_protein_filtered_df = stmts_by_protein_df[stmts_by_protein_df.name.isin(target_proteins)]
    else:
        stmts_by_protein_filtered_df = stmts_by_protein_df

    return stmts_by_protein_df, stmts_by_protein_filtered_df



def graph_barchart(filtered_df):
    """Visualize frequnecy of interaction types among protiens that have direct
       INDRA relationship to target
    
    Parameters
    ----------
    filtered_df : dataframe
        Contains INDRA relationships for target protein filtered by 
        "protein_list" genes

    Returns
    -------
    None.

    """
    type_counts = filtered_df["indra_type"].value_counts()
    type_counts.plot.bar()
    plt.xlabel("Interaction Type")
    plt.ylabel("Frequency")
    plt.title("Frequency of Type of Interaction With Target")
    plt.show()


def download_indra_htmls(filtered_df):
    '''Method to get INDRA statements for proteins of interest using html assembler

    Parameters
    ----------
    filtered_df: dataframe 
        Contains INDRA relationships for target protein filtered by                 
        "protein_list" genes

    Returns
    -------
    None.

    '''
    json_list = filtered_df["stmt_json"].values
    protein_names = filtered_df["name"].values
    
    # iterates through the gene name and json strings for each gene 
    for name, strings, index in zip(protein_names, json_list, range(len(protein_names))):
        stmt_jsons = []
        # iterates through the individual json string within the statements for each gene 
        # and converts it to an INDRA statement object
        stmt_jsons.append(json.loads(strings))
        stmts = stmts_from_json(json_in=stmt_jsons)
    
        # uses HtmlAssembler to get html pages of INDRA statements for each gene 
        ha = HtmlAssembler(stmts, title='Statements for %s' % name, db_rest_url='https://db.indra.bio')
        ha.save_model('%s_statements.html' % (name+str(index)))


def get_gene_id(source_protein):
    """Return HGNC id for protein of interest

    Parameters
    ----------
    protein_name: string
        The protein of interest in relation to protien list user enters

    Returns
    -------
    gene_id: string
        The HGNC id for the protein of interest

    """
    source_hgnc_id = hgnc_client.get_hgnc_id(source_protein)
    if not source_hgnc_id:
        source_hgnc_id = hgnc_client.get_current_hgnc_id(source_protein)
        if not source_hgnc_id:
            print("%s is not a valid gene name" % source_protein)
            return None
    return source_hgnc_id

def get_gene_ids(target_proteins):
    """Return HGNC ids for all proteins in the list

    Parameters
    ----------
    protein_list: list
        Contains proteins user enters to analyze in relation to target

    Returns
    -------
    """
    target_hgnc_ids = []
    for protein in target_proteins:
        hgnc_id = get_gene_id(protein)
        if hgnc_id:
            target_hgnc_ids.append(hgnc_id)
    return target_hgnc_ids


def shared_pathway(id_df, target_id, source_protein):
    """Find shared pathways between list of genes and target protien 
    
    Parameters
    ----------
    id_df: dataframe
        Contains HGNC ids for protein_list protein list
    target_id: string 
        The target proteins HGNC id
    source_protein: string
        The protein of interest in relation to protien list 
    
    Returns
    -------
    none
        
    """
    # iterates through ids and names of protein_list genes 
    for ids, names in zip(id_df["gene_id"].values, id_df["name"].values):
        # gets the numerical part of the string
        gene_id = ids[5:]
        result = get_shared_pathways_for_genes((("HGNC", gene_id),("HGNC", target_id)))
        if not result:
            print("\nThere are no shared pathways for", names, "and", source_protein)
        else:
            print("\nHere are the shared pathways for", names, "and", source_protein)
            print(result)


def child_of_target(id_df, target_id, source_protein):
    """ Determine if any gene in gene list isa/partof the target protein 
    Parameters
    ----------
    id_df : dataframe
        Contains HGNC ids for protein_list
    target_id : string 
        The target proteins HGNC id
    source_protein : string
        The protein of interest in relation to protien list user enters

    Returns
    -------
    None.

    """
    #iterates through the ids and names of the protein_list proteins 
    for ids, names in zip(id_df["gene_id"].values, id_df["name"].values):
       # gets the numerical part of the string only
       id = ids[5:]
       
       # uses isa_or_partof() to determine if protein is a child of target protein
       result = isa_or_partof(("HGNC", id),("HGNC", target_id))

       if result == True:
           print("\n", names, "and", source_protein, "are a part of the same family") 
           print(result)
       else: 
           print("\n",names, "and", source_protein, "are not a part of the same family") 
      

def get_go_terms_for_target(target_id):
    """ This method gets the go terms for the target protein
    Parameters
    ----------
    none

    Returns
    -------
    target_go: list 
        Contains the GO terms for target protein
    go_nodes: list 
        List of node objects that has information about GO terms for target protein
        
    """
    # these are the GO terms for target protein
    go_nodes = get_go_terms_for_gene(("HGNC", target_id))
    target_go = []
    # iterates through the genes in the list
    for genes in go_nodes:
        # changes the type to string and splits it
        text = str(genes)
        words = text.split()  
        # iterates through each word in the list of strings
        for word in words:
            # if statement to get just the gene name
            if word.startswith("id:"):
                target_go.append(word[7:-2].lower())
                
    return target_go, go_nodes


# for now this code needs to have a downloaded csv, but if there is eventually a rest api 
# for discrete gene analysis data, the way the data is loaded can be changed
def shared_bioentities(protein_df):
    """This method uses the indra_upstream csv to get a dataframe that is the 
        intersection of the upstream molecules and the bioentities that target 
        protein has direct INDRA relationships with and the bioentities that 
        target protein has direct INDRA relationships with
    
    Parameters
    ----------
    protein_df: dataframe 
        Contains all bioentities target protien has a direct INDRA relationship with 

    Returns
    -------
    shared_proteins: list
        list of shared bioentities between the indra_upstream results 
        and bioenties that have direct INDRA relationships with target protein
    
    shared_entities: dataframe 
        The filtered the indra_upstream_df using the shared_protiens list 
        (you can pick whether you want to filter the indra_upstream_df or 
        protein_df which contains all bioentities that target protein has a 
        direct INDRA relationship with)
        
    """
    # downloaded the upstream gene list analysis as a csv
    indra_upstream_df = pd.read_csv("/Users/ariaagarwal/Desktop/discrete.csv")
    
    # list that are shared entities between indra_upstream for gene set and 
    # proteins that have a direct INDRA relationship with target protein
    shared_proteins = list((set(indra_upstream_df["Name"].values)).intersection
                           (set(protein_df["name"].values)))
    df_list = []
    for i, j in enumerate(shared_proteins):
        # can pick if you want to filter from protein_df (which has proteins 
        #that have INDRA relationships to target) or indra_upstream_df 
            df_list.append(indra_upstream_df[indra_upstream_df["Name"] == shared_proteins[i]])
            shared_entities = pd.concat(df_list)
    shared_entities = shared_entities.reset_index()

    # code if want to filter for specific type of bioentity 
    # ex: protein_family_complex, small_molecule ect.
    
     #for num, type in enumerate(shared_entities["type"].values):
        #if type[0] == "protein_family_complex":
            #print(shared_entities.iloc[num])
    
    return shared_proteins, shared_entities
    

def finding_protein_complexes(target_go):
    """This method finds the shared go terms between the gene list and target 
        proteins GO terms again the data is downloaded from the discrete gene 
        analysis is as csv file
        
    Parameters
    ----------
    target_go: list 
        GO terms for Target protein

    Returns
    -------
    shared_df: dataframe
        Contains shared bioentities that have the same go terms 
        between the GO terms provided from the gene analysis and GO terms 
        associated with target protein
        
    """
    
    # loads data fron csv file
    go_terms_df = pd.read_csv("/Users/ariaagarwal/Desktop/goterms.csv")
    df_list = []
    # gets list of shared go terns between protein list and target protien
    shared_go = list((set(go_terms_df["CURIE"]).intersection(set(target_go))))
    
    # filters the target's go_term dataframe using the shared go term list 
    for i, j in enumerate(shared_go):
        df_list.append(go_terms_df[go_terms_df["CURIE"] == shared_go[i]])
    shared_complexes_df = pd.concat(df_list)
    
    return shared_complexes_df


 # did not perform analysis because shared pathways was already explored 
def gene_pathways():
    """ This method creates combined dataframe of REACTOME and Wikipathways
    provided by gene analysis for gene list
   
    Parameters
    ----------
    none

    Returns
    -------
    pathways_df : dataframe
        This dataframe contains the combined wikipathways and reactome
        pathways for the gene list 

    """
    reactome_df = pd.read_csv("/Users/ariaagarwal/Desktop/reactome.csv")
    wikipathways_df = pd.read_csv("/Users/ariaagarwal/Desktop/wikipathways.csv")
    pathways_df = pd.concat([reactome_df, wikipathways_df])
    
    return pathways_df


def graph_boxplots(shared_complexes_df,shared_entities):
    """ This method creates boxplots to visualize p and q values for 
        shared complexes/GO terms and bioentiies 
    

    Parameters
    ----------
    shared_complexes_df : dataframe
        Contains shared bioentities that have the same go terms 
        between the GO terms provided from the gene analysis and GO terms 
        associated with target protein.
    shared_entities : dataframe
        The filtered the indra_upstream_df using the shared_protiens list 
        (you can pick whether you want to filter the indra_upstream_df or 
        protein_df which contains all bioentities that target protein has a 
        direct INDRA relationship with).

    Returns
    -------
    None.

    """
    
    # plots boxplots for each type of graph 
    plt.title("P-values for Shared Complexes")
    shared_complexes_df.boxplot(column=["p-value"])
    plt.show()
    plt.title("Q-values for Shared Complexes")
    shared_complexes_df.boxplot(column=["q-value"])
    plt.show()

    plt.title("P-values for Shared Bioentities")
    shared_entities.boxplot(column=["p-value"])
    plt.show()
    
    plt.title("Q-values for Shared Bioentities")
    shared_entities.boxplot(column=["q-value"])
    plt.show()


def run_analysis(source_hgnc_id, target_hgnc_ids):
    # to get dataframe with protiens that target has INDRA rel with filtered by users gene list
    stmts_by_protein_df, stmts_by_protein_filtered_df = get_stmts_from_source(source_hgnc_id, target_hgnc_ids)
    print("\nThis is a dataframe of protiens that have INDRA relationships with ",
         source_hgnc_id, " that have been filtered for the protein list")
    print(stmts_by_protein_filtered_df)

    # visualize frequnecy of interaction types among protiens that have direct
    # INDRA relationship to target
    graph_barchart(filtered_df)

    # to get INDRA statements for protiens that have direct INDRA rel with target
    download_indra_htmls(filtered_df)

    # to get gene ids for users gene list and target protein
    id_df, target_id = get_gene_ids(protein_list, source_protein)

    # to find shared pathways between users gene list and target protein
    shared_pathway(id_df, target_id, source_protein)

    # which proteins of interest are part of the same protien family complex
    # as the target
    child_of_target(id_df, target_id, source_protein)

    # to get go term ids for target gene
    target_go, go_nodes = get_go_terms_for_target(target_id)

    # finds shared upstream bioentities between the users gene list and target protein
    shared_proteins, shared_entities = shared_bioentities(protein_df)
    print("These are the shared upstream bioentities between the gene list and",
         source_protein)
    print(shared_entities)

    # finds shared bioentities between users gene list and target protein using GO terms
    shared_complexes_df = finding_protein_complexes(target_go)
    print("These are shared complexes between the gene list and", source_protein)
    print(shared_complexes_df)

    # gets a list of reactome and wikipathways for shared genes
    pathways_df = gene_pathways()

    graph_boxplots(shared_complexes_df,shared_entities)


def main():
    # the protien list the user wants to analyze in relationship to target protein
    target_protein_names = \
        ['GLCE', 'ACSL5', 'APCDD1', 'ADAMTSL2', 'CALML3', 'CEMIP2',
         'AMOT', 'PLA2G4A', 'RCN2', 'TTC9', 'FABP4', 'GPCPD1', 'VSNL1',
         'CRYBB1', 'PDZD8', 'FNDC3A']


    # the protein of interest in relation to protien list user enters
    source_protein_name = "CTNNB1"

    source_hgnc_id = get_gene_id(source_protein_name)
    target_hgnc_ids = get_gene_ids(target_protein_names)
    
    print(source_hgnc_id,target_hgnc_ids)
    if not source_hgnc_id or not target_hgnc_ids:
        print("Cannot perform analysis due to invalid gene names")
        return
   
    
    run_analysis(source_hgnc_id, target_hgnc_ids)


if __name__ == '__main__':
    client = Neo4jClient()
    main()
