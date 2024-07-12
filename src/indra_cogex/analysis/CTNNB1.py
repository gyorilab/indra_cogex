#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CTTNB1 Exploration

Exploring how a unique set of protiens relate to CTNNB1/target protein through 
INDRA statements, exploring pathway membership,determining if any of the proteins 
belong to the same protein family/complex as the target and using 
INDRA discrete gene list analysis results

@author: ariaagarwal
"""

from indra_cogex.client import Neo4jClient
import json
client = Neo4jClient()
from indra.assemblers.html import HtmlAssembler
import json
from indra.statements import *
import pandas as pd
from indra_cogex.client import *

# for the sake of the CTNNB1 exploration I use 2 gene lists to create
# the list of proteins to be analyzed, but in general this function may not
# be necessary since the user should be able to enter a single list
def get_unique_proteins(paper_proteins, top_25):
    """
    Parameters
    ----------
    paper_proteins : list that contains names of proteins from paper
    top_25: list that contains protien names of top 25 protiens given

    Returns
    -------
    bcat_pathway: list that contains given protein names involved in the pathway 
    from google search
    unique: list that contains proteins that are in the top_25 list but not paper_protiens
        
    """

    # unique defined as proteins that are in the top 25 list but not in the paper
    unique = [x for x in top_25 if x not in paper_proteins]

    return unique
    

def find_indra_relationships(target_protein, unique):
    """
    Parameters
    ----------
    target_protein: string, the protein of interest in relation to protien list user enters
    unique: list that contains proteins in the top_25 list but not paper_protiens
        
    Returns
    -------
    combined_df: dataframe that contains INDRA relationships for CTNNB1 filtered
    by "unique" genes
    protein_df: unfiltered dataframe that contains all INDRA relationships for CTNNB1  
    """
    
    # cypher to get dataframe with all proteins that have INDRA relationship with CTNNB1
    cypher = f"""MATCH p=(n:BioEntity)-[r:indra_rel]->(m:BioEntity) 
    WHERE n.name = '{target_protein}'
    RETURN m.name, r.stmt_json, m.type, m.id, r.stmt_type"""
    
    proteins = client.query_tx(cypher)
    protein_df = pd.DataFrame(proteins, columns=["name", "stmt_json", "type", "id", "type"])
    
    df_list = []
    protein_list = protein_df["name"].values
    
    # filters the dataframe that contains all INDRA relationships for target protein 
    # for genes in the "unique" list
    for gene in unique:
        if gene in protein_list:
           df_list.append(protein_df[protein_df["name"] == gene])
           
    # combines dataframes for each gene into single dataframe
    combined_df = pd.concat(df_list, ignore_index=True)
    
    return combined_df, protein_df


# method to get INDRA statements for proteins of interest 
def get_indra_statements(combined_df):
    '''

    Parameters
    ----------
    combined_df: dataframe that contains INDRA relationships for CTNNB1 filtered
    by "unique" genes

    Returns
    -------
    None.

    '''
    json_list = combined_df["stmt_json"].values
    protein_names = combined_df["name"].values
    
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


# method to get gene ids for protiens of interest
def get_gene_ids(unique, target_protein):
    """
    Parameters
    ----------
    unique: list that contains proteins in the top_25 list but not paper_protiens

    Returns
    -------
    id_df: dataframe that contains HGNC ids for unique protein list
    target_id: string that is the target proteins HGNC id
        
    """
    id_df_list = []
    
    # iterates through the gene names
    for names in unique:
        
        # cypher query to get the gene ids 
        cypher = f"""MATCH p=(n:BioEntity) WHERE n.name = '{names}' 
        AND n.id starts with 'hgnc' RETURN n.name, n.id"""
        results = client.query_tx(cypher)
        
        # save and loads results into a dataframe for each gene id
        id_df_list.append(pd.DataFrame(results, columns=["name", "gene_id"]))
        
    # combines the dataframes into a single dataframe
    id_df = pd.concat(id_df_list, ignore_index=True)  
    
    target_id_cypher = f"""MATCH p=(n:BioEntity)-[r:indra_rel]->(m:BioEntity) 
    WHERE n.name = '{target_protein}' RETURN n.id LIMIT 1"""
    target_results = client.query_tx(target_id_cypher)
    target_id = target_results[0][0][5:]
    
    return id_df, target_id


def shared_pathway(id_df, target_id, target_protein):
    """
    Parameters
    ----------
    id_df: dataframe that contains HGNC ids for unique protein list
    target_id: string that is the target proteins HGNC id
    target_protein: string, the protein of interest in relation to protien list user enters
    
    Returns
    -------
    none
        
    """
    # iterates through ids and names of unique genes 
    for ids, names in zip(id_df["gene_id"].values, id_df["name"].values):
        # gets the numerical part of the string
        gene_id = ids[5:]
        result = get_shared_pathways_for_genes((("HGNC", gene_id),("HGNC", target_id)))
        if not result:
            print("\nThere are no shared pathways for", names, "and", target_protein)
        else:
            print("\nHere are the shared pathways for", names, "and", target_protein)
            print(result)


def child_of_target(id_df, target_id, target_protein):
    '''
    Parameters
    ----------
    id_df : dataframe that contains HGNC ids for unique protein list
    target_id : tring that is the target proteins HGNC id
    target_protein : string, the protein of interest in relation to protien list user enters

    Returns
    -------
    None.

    '''
    #iterates through the ids and names of the unique proteins 
    for ids, names in zip(id_df["gene_id"].values, id_df["name"].values):
       # gets the numerical part of the string only
       id = ids[5:]
       
       # uses isa_or_partof() to determine if protein is a child of CTNNB1
       result = isa_or_partof(("HGNC", id),("HGNC", target_id))

       if result == True:
           print("\n", names, "and", target_protein, "are a part of the same family") 
           print(result)
       else: 
           print("\n",names, "and", target_protein, "are not a part of the same family") 
      

def get_go_terms_for_target(target_id):
    """
    Parameters
    ----------
    none

    Returns
    -------
    target_go: contains list of GO terms for CTNNB1
    go_nodes: contains list of node objects that has information about GO terms for CTNNB1
        
    """
    # these are the GO terms for CTNNB1
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


# this method uses the indra_upstream csv to get a dataframe that is the intersection
# of the upstream molecules and the bioentities that target protein has direct INDRA relationships with
# for now this code needs to have a downloaded csv, but if there is eventually a rest api 
# for discrete gene analysis data, the way the data is loaded can be changed
def shared_entities(protein_df):
    """
    Parameters
    ----------
    protein_df: dataframe which contains all bioentities target protien has a 
    direct INDRA relationship with 

    Returns
    -------
    shared_proteins: list of shared bioentities between the indra_upstream results 
    and bioenties that have direct INDRA relationships with CTNNB1/target protein
    
    shared_indra: dataframe that is the filtered the indra_upstream_df using the shared_protiens list 
    (you can pick whether you want to filter the indra_upstream_df or protein_df which 
    contains all bioentities that CTNNB1 has a direct INDRA relationship with)
        
    """
    # downloaded the upstream gene list analysis as a csv
    indra_upstream_df = pd.read_csv("/Users/ariaagarwal/Desktop/discrete.csv")
    
    # list that are shared entities between indra_upstream for gene set and 
    # proteins that have a direct INDRA relationship with CTNNB1/target protein
    shared_proteins = list((set(indra_upstream_df["Name"].values)).intersection
                           (set(protein_df["name"].values)))
    df_list = []
    for i, j in enumerate(shared_proteins):
        # can pick if you want to filter from protein_df (which has proteins 
        #that have INDRA relationships to CTNNB1) or indra_upstream_df 
            df_list.append(indra_upstream_df[indra_upstream_df["Name"] == shared_proteins[i]])
            shared_indra = pd.concat(df_list)
    shared_indra = shared_indra.reset_index()

    # code if want to filter for specific type of bioentity 
    # ex: protein_family_complex, small_molecule ect.
    
     #for num, type in enumerate(shared_indra["type"].values):
        #if type[0] == "protein_family_complex":
            #print(shared_indra.iloc[num])
    
    return shared_proteins, shared_indra
    

# this method finds the shared go terms between the gene list and CTNNB1s/target proteins GO terms
# again the data is downloaded from the discrete gene analysis is as csv file
def finding_protein_complexes(target_go):
    """
    Parameters
    ----------
    target_go: list of GO terms for CTNNB1/target protein

    Returns
    -------
    shared_df = dataframe that contains shared bioentities that have the same go terms 
    between the GO terms provided from the gene analysis and GO terms associated with CTNNB1
        
    """
    go_terms_df = pd.read_csv("/Users/ariaagarwal/Desktop/goterms.csv")
    df_list = []
    shared_go = list((set(go_terms_df["CURIE"]).intersection(set(target_go))))
    for i, j in enumerate(shared_go):
        df_list.append(go_terms_df[go_terms_df["CURIE"] == shared_go[i]])
    shared_complexes_df = pd.concat(df_list)
    
    return shared_complexes_df

# combined dataframe of REACTOME and Wikipathways provided by gene analysis for gene list
# did not perform analysis because shared pathways was already explored 
def gene_pathways():
    reactome_df = pd.read_csv("/Users/ariaagarwal/Desktop/reactome.csv")
    wikipathways_df = pd.read_csv("/Users/ariaagarwal/Desktop/wikipathways.csv")
    pathways_df = pd.concat([reactome_df, wikipathways_df])
    
    return pathways_df

def main():
   # 2 lists of proteins are used, the proteins listed in the paper and
   # statistically top 25, and the proteins involved in the patwhay
   paper_proteins = ["CTNNB1", "LEF1", "CTNNA2", "EPHA7", "LRP4", "NOTUM", "DKK4", 
                     "JAG1", "PSEN2", "RBPJ", "HELZ2", "KIAA0513", "LSP1", "VWA2", 
                     "CXCL14", "GNE", "GTF2F1", "TLK1", "ZNF638", "HDAC2", "HDAC5",
                     "NCSTN", "NUMB", "AXIN1", "FZD1", "GNAI1", "TP53"]
    
   top_25 = ["VWA2", "LRP4", "CTNNB1", "GLCE", "ACSL5", "NOTUM", "APCDD1", "DKK4", 
             "EPHA7", "CTNNA2", "ADAMTSL2", "CALML3","CEMIP2", "AMOT", "CXCL14", 
             "PLA2G4A", "RCN2", "TTC9", "FABP4", "GPCPD1", "VSNL1", "CRYBB1", 
             "LEF1", "PDZD8", "FNDC3A"]
   
   # "unqiue" would be the protien list the user enters, but for the sake of CTNNB1
   # exploration these are the protiens mentioned in top_25 list but not the paper
   unique = get_unique_proteins(paper_proteins, top_25) 
   
   # the protein of interest in relation to protien list user enters
   target_protein = "CTNNB1"
   # to get dataframe with protiens that target has INDRA rel with filtered by users gene list
   combined_df, protein_df = find_indra_relationships(target_protein, unique)
   print("\nThis is a dataframe of protiens that have INDRA relationships with ", 
         target_protein, " that have been filtered for the protein list")
   print(combined_df)
   
   # to get INDRA statements for protiens that have direct INDRA rel with target
   get_indra_statements(combined_df)
   
   # to get gene ids for users gene list and target protein 
   id_df, target_id = get_gene_ids(unique, target_protein)
 
    # to find shared pathways between users gene list and target protein 
   shared_pathway(id_df, target_id, target_protein)
   
   # which proteins of interest are part of the same protien family complex
   # as the target
   child_of_target(id_df, target_id, target_protein)
   
   # to get go term ids for target gene
   target_go, go_nodes = get_go_terms_for_target(target_id)
  
   # finds shared upstream bioentities between the users gene list and target protein
   shared_proteins, shared_indra = shared_entities(protein_df)
   print("These are the shared upstream bioentities between the gene list and",
         target_protein)
   print(shared_indra)
   
   # finds shared bioentities between users gene list and target protein using GO terms
   shared_complexes_df = finding_protein_complexes(target_go)
   print("These are shared complexes between the gene list and", target_protein)
   print(shared_complexes_df)
   
   # gets a list of reactome and wikipathways for shared genes
   pathways_df = gene_pathways()
   
main()


