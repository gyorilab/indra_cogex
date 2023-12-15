import json

import pytest
from indra.statements import Statement, Evidence

from indra_cogex.client.queries import *
from indra_cogex.client.queries import (
    _filter_out_medscan_evidence,
    _get_ev_dict_from_hash_ev_query,
    _get_mesh_child_terms,
)
from indra_cogex.representation import Node, norm_id

from .test_neo4j_client import _get_client


@pytest.mark.nonpublic
def test_get_genes_in_tissue():
    # Single query
    client = _get_client()
    genes = get_genes_in_tissue(("UBERON", "UBERON:0002349"), client=client)
    assert genes
    node0 = genes[0]
    assert isinstance(node0, Node)
    assert node0.db_ns == "HGNC"
    assert ("HGNC", "9891") in {g.grounding() for g in genes}


@pytest.mark.nonpublic
def test_get_tissues_for_gene():
    # Single query
    client = _get_client()
    tissues = get_tissues_for_gene(("HGNC", "9896"), client=client)
    assert tissues
    node0 = tissues[0]
    assert isinstance(node0, Node)
    assert node0.db_ns in {"UBERON", "CL"}
    assert ("UBERON", "UBERON:0002349") in {g.grounding() for g in tissues}


@pytest.mark.nonpublic
def test_is_gene_in_tissue():
    client = _get_client()
    gene = ("HGNC", "9896")  # RBM10
    tissue = ("UBERON", "UBERON:0035841")  # esophagogastric junction muscularis propria
    assert is_gene_in_tissue(gene, tissue, client=client)


@pytest.mark.nonpublic
def test_get_go_terms_for_gene():
    client = _get_client()
    gene = ("HGNC", "2697")  # DBP
    go_terms = get_go_terms_for_gene(gene, client=client)
    assert go_terms
    node0 = go_terms[0]
    assert isinstance(node0, Node)
    assert node0.db_ns == "GO"
    assert ("GO", "GO:0000978") in {g.grounding() for g in go_terms}


@pytest.mark.nonpublic
def test_get_genes_for_go_term():
    # Single query
    client = _get_client()
    go_term = ("GO", "GO:0000978")
    genes = get_genes_for_go_term(go_term, client=client)
    assert genes
    node0 = genes[0]
    assert isinstance(node0, Node)
    assert node0.db_ns == "HGNC"
    assert ("HGNC", "2697") in {g.grounding() for g in genes}


@pytest.mark.nonpublic
def test_is_go_term_for_gene():
    # Single query
    client = _get_client()
    go_term = ("GO", "GO:0000978")
    gene = ("HGNC", "2697")  # DBP
    assert is_go_term_for_gene(gene, go_term, client=client)


@pytest.mark.nonpublic
def test_get_trials_for_drug():
    client = _get_client()
    drug = ("MESH", "C000489")
    trials = get_trials_for_drug(drug, client=client)
    assert trials
    assert isinstance(trials[0], Node)
    assert trials[0].db_ns == "CLINICALTRIALS"
    assert ("CLINICALTRIALS", "NCT00000674") in {t.grounding() for t in trials}


@pytest.mark.nonpublic
def test_get_trials_for_disease():
    client = _get_client()
    disease = ("MESH", "D007855")
    trials = get_trials_for_disease(disease, client=client)
    assert trials
    assert isinstance(trials[0], Node)
    assert trials[0].db_ns == "CLINICALTRIALS"
    assert ("CLINICALTRIALS", "NCT00011661") in {t.grounding() for t in trials}


@pytest.mark.nonpublic
def test_get_drugs_for_trial():
    client = _get_client()
    trial = ("CLINICALTRIALS", "NCT00000114")
    drugs = get_drugs_for_trial(trial, client=client)
    assert drugs
    assert drugs[0].db_ns in ["CHEBI", "MESH"]
    assert ("MESH", "D014810") in {d.grounding() for d in drugs}


@pytest.mark.nonpublic
def test_get_diseases_for_trial():
    client = _get_client()
    trial = ("CLINICALTRIALS", "NCT00000114")
    diseases = get_diseases_for_trial(trial, client=client)
    assert diseases
    assert isinstance(diseases[0], Node)
    assert diseases[0].db_ns == "MESH"
    assert ("MESH", "D012174") in {d.grounding() for d in diseases}


@pytest.mark.nonpublic
def test_get_pathways_for_gene():
    client = _get_client()
    gene = ("HGNC", "16812")
    pathways = get_pathways_for_gene(gene, client=client)
    assert pathways
    assert isinstance(pathways[0], Node)
    assert pathways[0].db_ns in {"WIKIPATHWAYS", "REACTOME"}
    assert ("WIKIPATHWAYS", "WP5037") in {p.grounding() for p in pathways}


@pytest.mark.nonpublic
def test_get_shared_pathways_for_gene():
    client = _get_client()
    gene1 = ("HGNC", "1097")
    gene2 = ("HGNC", "6407")
    pathways = get_shared_pathways_for_genes([gene1, gene2], client=client)
    assert pathways
    assert isinstance(pathways[0], Node)
    assert pathways[0].db_ns in {"WIKIPATHWAYS", "REACTOME"}
    assert ("WIKIPATHWAYS", "WP4685") in {p.grounding() for p in pathways}
    assert ("REACTOME", "R-HSA-6802952") in {p.grounding() for p in pathways}


@pytest.mark.nonpublic
def test_get_genes_for_pathway():
    client = _get_client()
    pathway = ("WIKIPATHWAYS", "WP5037")
    genes = get_genes_for_pathway(pathway, client=client)
    assert genes
    assert isinstance(genes[0], Node)
    assert genes[0].db_ns == "HGNC"
    assert ("HGNC", "16812") in {g.grounding() for g in genes}


@pytest.mark.nonpublic
def test_is_gene_in_pathway():
    client = _get_client()
    gene = ("HGNC", "16812")
    pathway = ("WIKIPATHWAYS", "WP5037")
    assert is_gene_in_pathway(gene, pathway, client=client)


@pytest.mark.nonpublic
def test_get_side_effects_for_drug():
    client = _get_client()
    drug = ("CHEBI", "CHEBI:29108")
    side_effects = get_side_effects_for_drug(drug, client=client)
    assert side_effects
    assert isinstance(side_effects[0], Node)
    assert side_effects[0].db_ns in ["GO", "UMLS", "MESH", "HP"]
    assert ("UMLS", "C3267206") in {s.grounding() for s in side_effects}


@pytest.mark.nonpublic
def test_get_drugs_for_side_effect():
    client = _get_client()
    side_effect = ("UMLS", "C3267206")
    drugs = get_drugs_for_side_effect(side_effect, client=client)
    assert drugs
    assert isinstance(drugs[0], Node)
    assert drugs[0].db_ns in ["CHEBI", "MESH"]
    assert ("CHEBI", "CHEBI:29108") in {d.grounding() for d in drugs}


@pytest.mark.nonpublic
def test_is_side_effect_for_drug():
    client = _get_client()
    drug = ("CHEBI", "CHEBI:29108")
    side_effect = ("UMLS", "C3267206")
    assert is_side_effect_for_drug(drug, side_effect, client=client)


@pytest.mark.nonpublic
def test_get_ontology_child_terms():
    client = _get_client()
    term = ("MESH", "D007855")
    children = get_ontology_child_terms(term, client=client)
    assert children
    assert isinstance(children[0], Node)
    assert children[0].db_ns == "MESH"
    assert ("MESH", "D020264") in {c.grounding() for c in children}


@pytest.mark.nonpublic
def test_get_ontology_parent_terms():
    client = _get_client()
    term = ("MESH", "D020263")
    parents = get_ontology_parent_terms(term, client=client)
    assert parents
    assert isinstance(parents[0], Node)
    assert parents[0].db_ns == "MESH"
    assert ("MESH", "D007855") in {p.grounding() for p in parents}


@pytest.mark.nonpublic
def test_isa_or_partof():
    client = _get_client()
    term = ("MESH", "D020263")
    parent = ("MESH", "D007855")
    assert isa_or_partof(term, parent, client=client)


@pytest.mark.nonpublic
def test_get_mesh_child_terms_empty():
    client = _get_client()
    term = ("MESH", "D015002")
    child_terms = _get_mesh_child_terms(term, client=client)
    assert isinstance(child_terms, set)
    assert child_terms == set()


@pytest.mark.nonpublic
def test_get_mesh_child_terms_nonempty():
    client = _get_client()
    term = ("MESH", "D007855")
    child_terms = _get_mesh_child_terms(term, client=client)
    assert isinstance(child_terms, set)
    assert len(child_terms) > 0
    assert list(child_terms)[0].startswith("mesh:")


@pytest.mark.nonpublic
def test_get_pmids_for_mesh():
    # Single query
    client = _get_client()
    pmids = get_pmids_for_mesh(("MESH", "D015002"), client=client)
    assert pmids
    assert isinstance(pmids[0], Node)
    assert pmids[0].db_ns == "PUBMED"
    assert ("PUBMED", "14915949") in {p.grounding() for p in pmids}


@pytest.mark.nonpublic
def test_get_mesh_ids_for_pmid():
    # Single query
    client = _get_client()
    pmid = ("PUBMED", "27890007")
    mesh_ids = get_mesh_ids_for_pmid(pmid, client=client)
    assert mesh_ids
    assert isinstance(mesh_ids[0], Node)
    assert mesh_ids[0].db_ns == "MESH"
    assert ("MESH", "D000544") in {m.grounding() for m in mesh_ids}


@pytest.mark.nonpublic
def test_get_mesh_ids_for_pmids():
    """Make a query over multiple pmids"""
    client = _get_client()
    pmids = ["27890007", "27890006"]
    mesh_ids = get_mesh_ids_for_pmids(pmids, client=client)
    assert isinstance(mesh_ids, dict)
    assert all(pmid in mesh_ids for pmid in pmids)
    assert "D000544" in mesh_ids["27890007"]


@pytest.mark.nonpublic
def test_get_evidence_obj_for_mesh_id():
    client = _get_client()
    mesh_id = ("MESH", "D015002")
    evidence_dict = get_evidences_for_mesh(mesh_id, client=client)
    assert len(evidence_dict)
    assert isinstance(list(evidence_dict.values())[0][0], Evidence)


@pytest.mark.nonpublic
def test_get_evidence_obj_for_stmt_hash():
    # Single query
    stmt_hash = -21655886415682961
    client = _get_client()
    ev_objs = get_evidences_for_stmt_hash(stmt_hash, client=client)
    assert ev_objs
    assert isinstance(ev_objs[0], Evidence)


@pytest.mark.nonpublic
def test_get_evidence_obj_for_stmt_hashes():
    # Note: These statements have 3+5 evidences
    # Single query
    stmt_hashes = [-21655886415682961, 18250443097459273]
    client = _get_client()
    ev_dict = get_evidences_for_stmt_hashes(stmt_hashes, client=client)
    assert ev_dict
    assert set(ev_dict.keys()) == {-21655886415682961, 18250443097459273}
    assert ev_dict[-21655886415682961]
    assert ev_dict[18250443097459273]
    assert isinstance(ev_dict[-21655886415682961][0], Evidence)
    assert isinstance(ev_dict[18250443097459273][0], Evidence)


@pytest.mark.nonpublic
def test_get_stmts_for_pmid():
    # Two queries: first evidences, then the statements
    client = _get_client()
    term = ("PUBMED", "14898026")
    stmts = get_stmts_for_paper(term, client=client)
    assert stmts
    assert isinstance(stmts[0], Statement)


@pytest.mark.nonpublic
def test_get_stmts_for_pmids():
    # Two queries: first evidences, then the statements
    client = _get_client()
    pmids = ["14898026"]
    stmts = get_stmts_for_pmids(pmids, client=client)
    assert stmts
    assert isinstance(stmts[0], Statement)


@pytest.mark.nonpublic
def test_get_stmts_for_mesh_id_w_children():
    # Two queries:
    # 1. evidences for publications with pmid having mesh annotation
    # 2. statements for the evidences in 1
    client = _get_client()
    mesh_id = ("MESH", "D000068236")
    stmts = get_stmts_for_mesh(mesh_id, client=client)
    assert stmts
    assert isinstance(stmts[0], Statement)


@pytest.mark.nonpublic
def test_get_stmts_for_mesh_id_wo_children():
    # Two queries:
    # 1. evidences for publications with pmid having mesh annotation
    # 2. statements for the evidences in 1
    client = _get_client()
    mesh_id = ("MESH", "D000068236")
    stmts = get_stmts_for_mesh(mesh_id, include_child_terms=False, client=client)
    assert stmts
    assert isinstance(stmts[0], Statement)


@pytest.mark.nonpublic
def test_get_stmts_by_hashes():
    # Note: This statement has a ~500 of evidences
    # Two queries: first statements, then all the evidence for the statements
    stmt_hashes = [35279776755000170]
    client = _get_client()
    stmts = get_stmts_for_stmt_hashes(stmt_hashes, client=client)
    assert stmts
    assert isinstance(stmts[0], Statement)


@pytest.mark.nonpublic
def test_is_gene_mutated():
    client = _get_client()
    gene = ("HGNC", "8975")
    cell_line = ("CCLE", "BT20_BREAST")
    assert is_gene_mutated(gene, cell_line, client=client)


@pytest.mark.nonpublic
def test_drugs_for_target():
    client = _get_client()
    target = ("HGNC", "6840")
    drugs = get_drugs_for_target(target, client=client)
    assert drugs
    assert isinstance(drugs[0], Node)
    assert drugs[0].db_ns == "CHEBI"
    assert ("CHEBI", "CHEBI:90227") in {d.grounding() for d in drugs}


@pytest.mark.nonpublic
def test_drugs_for_targets():
    client = _get_client()
    target = ("HGNC", "6840")
    norm_target = norm_id(*target)
    drugs_dict = get_drugs_for_targets([target], client=client)
    assert drugs_dict
    assert isinstance(drugs_dict, dict)
    assert norm_target in drugs_dict
    assert isinstance(drugs_dict[norm_target], list)
    drug_node = list(drugs_dict.values())[0][0]
    assert isinstance(drug_node, Node)
    assert drug_node.db_ns == "CHEBI"
    assert ("CHEBI", "CHEBI:90227") in {
        n.grounding() for il in drugs_dict.values() for n in il
    }


@pytest.mark.nonpublic
def test_targets_for_drug():
    client = _get_client()
    drug = ("CHEBI", "CHEBI:90227")
    targets = get_targets_for_drug(drug, client=client)
    assert targets
    assert isinstance(targets[0], Node)
    assert targets[0].db_ns == "HGNC"
    assert ("HGNC", "6840") in {t.grounding() for t in targets}


@pytest.mark.nonpublic
def test_targets_for_drugs():
    client = _get_client()
    drug = ("CHEBI", "CHEBI:90227")
    target_dict = get_targets_for_drugs([drug], client=client)
    assert target_dict
    assert isinstance(target_dict, dict)
    norm_drug = list(target_dict.keys())[0]
    assert norm_drug.startswith("chebi")
    assert isinstance(target_dict[norm_drug], list)
    target_node = list(target_dict.values())[0][0]
    assert isinstance(target_node, Node)
    assert target_node.db_ns == "HGNC"
    assert ("HGNC", "6840") in {n.grounding() for il in target_dict.values()
                                for n in il}


@pytest.mark.nonpublic
def test_is_drug_target():
    client = _get_client()
    drug = ("CHEBI", "CHEBI:90227")
    target = ("HGNC", "6840")
    assert is_drug_target(drug, target, client=client)
    wrong_target = ("HGNC", "6407")
    assert not is_drug_target(drug, wrong_target, client=client)


def test_filter_out_medscan_evidence():
    ev = Evidence(source_api="reach").to_json()
    medscan_ev = Evidence(source_api="medscan").to_json()

    ev_list = _filter_out_medscan_evidence([ev, medscan_ev], remove_medscan=True)
    assert len(ev_list) == 1
    assert ev_list[0].equals(Evidence._from_json(ev))

    ev_list = _filter_out_medscan_evidence([ev, medscan_ev], remove_medscan=False)
    assert len(ev_list) == 2
    assert ev_list[0].equals(Evidence._from_json(ev))
    assert ev_list[1].equals(Evidence._from_json(medscan_ev))

    ev_list = _filter_out_medscan_evidence([medscan_ev], remove_medscan=True)
    assert len(ev_list) == 0


def test_get_ev_dict_from_hash_ev_query():
    ev = Evidence(source_api="reach").to_json()
    medscan_ev = Evidence(source_api="medscan").to_json()

    ev_dict = _get_ev_dict_from_hash_ev_query(
        [[123456, json.dumps(ev)], [654321, json.dumps(medscan_ev)]],
        remove_medscan=True,
    )
    assert ev_dict[123456]
    assert ev_dict[123456][0].equals(Evidence._from_json(ev))
    assert 654321 not in ev_dict

    ev_dict = _get_ev_dict_from_hash_ev_query(
        [[123456, json.dumps(ev)], [654321, json.dumps(medscan_ev)]],
        remove_medscan=False,
    )
    assert ev_dict[123456]
    assert ev_dict[123456][0].equals(Evidence._from_json(ev))
    assert ev_dict[654321]
    assert ev_dict[654321][0].equals(Evidence._from_json(medscan_ev))

    ev_dict = _get_ev_dict_from_hash_ev_query(
        [[654321, json.dumps(medscan_ev)]],
        remove_medscan=True,
    )
    assert 654321 not in ev_dict
    assert not ev_dict
