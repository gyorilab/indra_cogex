import pytest
from indra_cogex.client.queries import *
from indra.statements import *
from indra_cogex.representation import Node

from .test_neo4j_client import _get_client


@pytest.mark.nonpublic
def test_get_genes_in_tissue():
    # Single query
    client = _get_client()
    genes = get_genes_in_tissue(client, ("UBERON", "UBERON:0002349"))
    assert len(genes)
    node0 = genes[0]
    assert isinstance(node0, Node)
    assert node0.db_ns == 'HGNC'


@pytest.mark.nonpublic
def test_get_tissues_for_gene():
    # Single query
    client = _get_client()
    tissues = get_tissues_for_gene(client, ("HGNC", "9896"))
    assert len(tissues)
    node0 = tissues[0]
    assert isinstance(node0, Node)
    assert node0.db_ns == 'UBERON'


@pytest.mark.nonpublic
def test_is_gene_in_tissue():
    client = _get_client()
    gene = ("HGNC", "9896")  # RBM10
    tissue = ("UBERON", "UBERON:0035841")  # esophagogastric junction muscularis propria
    assert is_gene_in_tissue(client, gene, tissue)


@pytest.mark.nonpublic
def test_get_go_terms_for_gene():
    client = _get_client()
    gene = ("HGNC", "16813")  # PPP1R27
    go_terms = get_go_terms_for_gene(client, gene)
    assert len(go_terms)
    node0 = go_terms[0]
    assert isinstance(node0, Node)
    assert node0.db_ns == 'GO'


@pytest.mark.nonpublic
def test_get_genes_for_go_term():
    # Single query
    client = _get_client()
    go_term = ("GO", "GO:0000978")
    genes = get_genes_for_go_term(client, go_term)
    assert len(genes)
    node0 = genes[0]
    assert isinstance(node0, Node)
    assert node0.db_ns == 'HGNC'


@pytest.mark.nonpublic
def test_is_go_term_for_gene():
    # Single query
    client = _get_client()
    go_term = ("GO", "GO:0000978")
    gene = ("HGNC", "2697")  # DBP
    assert is_go_term_for_gene(client, gene, go_term)


@pytest.mark.nonpublic
def test_get_trials_for_drug():
    client = _get_client()
    drug = ("CHEBI", "CHEBI:27690")
    trials = get_trials_for_drug(client, drug)
    assert len(trials)
    assert isinstance(trials[0], Node)
    assert trials[0].db_ns == 'CLINICALTRIALS'


@pytest.mark.nonpublic
def test_get_trials_for_disease():
    client = _get_client()
    disease = ("MESH", "D007855")
    trials = get_trials_for_disease(client, disease)
    assert len(trials)
    assert isinstance(trials[0], Node)
    assert trials[0].db_ns == 'CLINICALTRIALS'


@pytest.mark.nonpublic
def test_get_drugs_for_trial():
    client = _get_client()
    trial = ("CLINICALTRIALS", "NCT00000114")
    drugs = get_drugs_for_trial(client, trial)
    assert len(drugs)
    assert drugs[0].db_ns in ['CHEBI', 'MESH']


@pytest.mark.nonpublic
def test_get_diseases_for_trial():
    client = _get_client()
    trial = ("CLINICALTRIALS", "NCT00000114")
    diseases = get_diseases_for_trial(client, trial)
    assert len(diseases)
    assert isinstance(diseases[0], Node)
    assert diseases[0].db_ns == 'MESH'


@pytest.mark.nonpublic
def test_get_pathways_for_gene():
    client = _get_client()
    gene = ("HGNC", "16812")
    pathways = get_pathways_for_gene(client, gene)
    assert len(pathways)
    assert isinstance(pathways[0], Node)
    assert pathways[0].db_ns == 'WIKIPATHWAYS'


@pytest.mark.nonpublic
def test_get_genes_for_pathway():
    client = _get_client()
    pathway = ("WIKIPATHWAYS", "WP5037")
    genes = get_genes_for_pathway(client, pathway)
    assert len(genes)
    assert isinstance(genes[0], Node)
    assert genes[0].db_ns == 'HGNC'


@pytest.mark.nonpublic
def test_is_gene_in_pathway():
    client = _get_client()
    gene = ("HGNC", "16812")
    pathway = ("WIKIPATHWAYS", "WP5037")
    assert is_gene_in_pathway(client, gene, pathway)


@pytest.mark.nonpublic
def test_get_side_effects_for_drug():
    client = _get_client()
    drug = ("CHEBI", "CHEBI:29108")
    side_effects = get_side_effects_for_drug(client, drug)
    assert len(side_effects)
    assert isinstance(side_effects[0], Node)
    assert side_effects[0].db_ns in ['GO', 'UMLS', 'MESH', 'HP']


@pytest.mark.nonpublic
def test_get_drugs_for_side_effect():
    client = _get_client()
    side_effect = ("UMLS", "C3267206")
    drugs = get_drugs_for_side_effect(client, side_effect)
    assert len(drugs)
    assert isinstance(drugs[0], Node)
    assert drugs[0].db_ns in ['CHEBI', 'MESH']


@pytest.mark.nonpublic
def test_is_side_effect_for_drug():
    client = _get_client()
    drug = ("CHEBI", "CHEBI:29108")
    side_effect = ("UMLS", "C3267206")
    assert is_side_effect_for_drug(client, drug, side_effect)


@pytest.mark.nonpublic
def test_get_ontology_child_terms():
    client = _get_client()
    term = ("MESH", "D007855")
    children = get_ontology_child_terms(client, term)
    assert len(children)
    assert isinstance(children[0], Node)
    assert children[0].db_ns == 'MESH'


@pytest.mark.nonpublic
def test_get_ontology_parent_terms():
    client = _get_client()
    term = ("MESH", "D020263")
    parents = get_ontology_parent_terms(client, term)
    assert len(parents)
    assert isinstance(parents[0], Node)
    assert parents[0].db_ns == 'MESH'


@pytest.mark.nonpublic
def test_isa_or_partof():
    client = _get_client()
    term = ("MESH", "D020263")
    parent = ("MESH", "D007855")
    assert isa_or_partof(client, term, parent)


@pytest.mark.nonpublic
def test_get_pmids_for_mesh():
    # Single query
    client = _get_client()
    pmids = get_pmids_for_mesh(client, ('MESH', 'D015002'))
    assert len(pmids)
    assert pmids[0].startswith('pubmed:')


@pytest.mark.nonpublic
def test_get_mesh_ids_for_pmid():
    # Single query
    client = _get_client()
    pmid = ("PUBMED", "27890007")
    mesh_ids = get_mesh_ids_for_pmid(client, pmid)
    assert len(mesh_ids)
    assert mesh_ids[0].startswith('mesh:')


@pytest.mark.nonpublic
def test_get_evidence_obj_for_stmt_hash():
    # Note: This statement has a ~500 evidences
    # Single query
    stmt_hash = '35279776755000170'
    client = _get_client()
    ev_objs = get_evidence_obj_for_stmt_hash(client, stmt_hash)
    assert len(ev_objs)
    assert isinstance(ev_objs[0], Evidence)


@pytest.mark.nonpublic
def test_get_evidence_obj_for_stmt_hashes():
    # Note: This statement has ~500 of evidences
    # Single query
    stmt_hashes = ['35279776755000170']
    client = _get_client()
    ev_dict = get_evidence_obj_for_stmt_hashes(client, stmt_hashes)
    assert len(ev_dict)
    assert list(ev_dict.keys())[0] == '35279776755000170'
    assert len(ev_dict['35279776755000170'])
    assert isinstance(ev_dict['35279776755000170'][0], Evidence)


@pytest.mark.nonpublic
def test_get_stmts_for_pmid():
    # Two queries: first evidences, then the statements
    client = _get_client()
    pmid = ("PUBMED", "14898026")
    stmts = get_stmts_for_pmid(client, pmid)
    assert len(stmts)
    assert isinstance(stmts[0], Inhibition)


@pytest.mark.nonpublic
def test_get_stmts_for_mesh_id():
    # Three queries:
    # 1. pmids with annotation
    # 2. evidences for publications with pmid in pmids from 1
    # 3. statements for the evidences in 2
    client = _get_client()
    mesh_id = ("MESH", "D000068236")
    stmts = get_stmts_for_mesh_id(client, mesh_id)
    assert len(stmts)
    assert isinstance(stmts[0], Activation)


@pytest.mark.nonpublic
def test_get_stmts_by_hashes():
    # Note: This statement has a ~500 of evidences
    # Two queries: first statements, then all the evidence for the statements
    stmt_hashes = ['35279776755000170']
    client = _get_client()
    stmts = get_stmts_for_stmt_hashes(client, stmt_hashes)
    assert len(stmts)
    assert isinstance(stmts[0], Inhibition)
