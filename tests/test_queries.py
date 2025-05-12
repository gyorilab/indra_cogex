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
    # GO:0000978 -
    # RNA polymerase II cis-regulatory region sequence-specific DNA binding
    go_term = ("GO", "GO:0000978")
    genes = get_genes_for_go_term(go_term, client=client)
    assert genes
    assert all(isinstance(node, Node) for node in genes)
    assert all(node.db_ns == "HGNC" for node in genes)
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
    drug = ("CHEBI", "CHEBI:135866")
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
def test_get_statements():
    client = _get_client()
    stmts, source_counts = get_statements(
        agent="MEK",
        other_agent="ERK",
        agent_role='subject',
        other_role='object',
        rel_types=["Phosphorylation", "Activation"],
        stmt_sources='reach',
        mesh_term=("MESH", 'D000818'),
        paper_term=('pubmed', '23356518'),
        client=client,
        limit=1000,
        evidence_limit=500,
        return_source_counts=True
    )
    assert stmts
    assert all(isinstance(stmt, Statement) for stmt in stmts)
    assert source_counts
    stmt_hashes = {stmt.get_hash() for stmt in stmts}
    assert stmt_hashes == source_counts.keys()
    for stmt_hash, sc in source_counts.items():
        assert all(v > 0 for v in sc.values())


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


@pytest.mark.nonpublic
def test_get_phenotypes_for_disease():
    client = _get_client()
    disease = ("doid", "0040093")
    phenotypes = get_phenotypes_for_disease(disease, client=client)
    phenotype_list = list(phenotypes)
    assert phenotype_list  # Check if list is not empty
    assert isinstance(phenotype_list[0], Node)
    assert phenotype_list[0].db_ns == "MESH"


@pytest.mark.nonpublic
def test_get_diseases_for_phenotype():
    client = _get_client()
    phenotype = ("hp", "0003138")
    diseases = get_diseases_for_phenotype(phenotype, client=client)
    disease_list = list(diseases)
    assert disease_list  # Check if list is not empty
    assert isinstance(disease_list[0], Node)
    assert disease_list[0].db_ns in ["DOID", "MESH"]  # Accept either namespace


@pytest.mark.nonpublic
def test_has_phenotype():
    client = _get_client()
    disease = ("doid", "0040093")
    phenotype = ("hp", "0003138")
    assert has_phenotype(disease, phenotype, client=client)


@pytest.mark.nonpublic
def test_get_genes_for_phenotype():
    client = _get_client()
    phenotype = ("MESH", "D009264")
    genes = get_genes_for_phenotype(phenotype, client=client)
    assert genes
    assert isinstance(genes[0], Node)
    assert genes[0].db_ns == "HGNC"


@pytest.mark.nonpublic
def test_get_phenotypes_for_gene():
    client = _get_client()
    gene = ("HGNC", "9896")
    phenotypes = get_phenotypes_for_gene(gene, client=client)
    assert phenotypes
    assert isinstance(phenotypes[0], Node)
    assert phenotypes[0].db_ns == "MESH"


@pytest.mark.nonpublic
def test_has_phenotype_gene():
    client = _get_client()
    phenotype = ("MESH", "D009264")
    gene = ("HGNC", "25126")
    assert has_phenotype_gene(phenotype, gene, client=client)


@pytest.mark.nonpublic
def test_get_markers_for_cell_type():
    client = _get_client()
    cell_type = ("cl", "0000020")
    markers = get_markers_for_cell_type(cell_type, client=client)
    marker_list = list(markers)
    assert marker_list
    assert isinstance(marker_list[0], Node)
    assert marker_list[0].db_ns == "HGNC"
    # Verify a specific marker we know exists
    assert ("HGNC", "11337") in [m.grounding() for m in marker_list]


@pytest.mark.nonpublic
def test_get_cell_types_for_marker():
    client = _get_client()
    marker = ("HGNC", "11337")
    cell_types = get_cell_types_for_marker(marker, client=client)
    cell_type_list = list(cell_types)
    assert cell_type_list
    assert isinstance(cell_type_list[0], Node)
    assert cell_type_list[0].db_ns == "CL"
    assert ("CL", "CL:0000020") in [c.grounding() for c in cell_type_list]


@pytest.mark.nonpublic
def test_is_marker_for_cell_type():
    client = _get_client()
    marker = ("HGNC", "11337")
    cell_type = ("cl", "0000020")
    assert is_marker_for_cell_type(marker, cell_type, client=client)


@pytest.mark.nonpublic
def test_get_publisher_for_journal():
    client = _get_client()
    journal = ("nlm", "100972832")
    publishers = get_publisher_for_journal(journal, client=client)
    publisher_list = list(publishers)
    assert publisher_list
    assert isinstance(publisher_list[0], Node)
    assert publisher_list[0].db_ns == "ISNI"
    assert ("ISNI", "0000000031304729") in [p.grounding() for p in publisher_list]


@pytest.mark.nonpublic
def test_get_journals_for_publisher():
    client = _get_client()
    publisher = ("isni", "0000000080461210")
    journals = get_journals_for_publisher(publisher, client=client)
    journal_list = list(journals)
    assert journal_list
    assert isinstance(journal_list[0], Node)
    assert journal_list[0].db_ns == "NLM"
    assert ("NLM", "8214119") in [j.grounding() for j in journal_list]


@pytest.mark.nonpublic
def test_is_journal_published_by():
    client = _get_client()
    journal = ("nlm", "100972832")
    publisher = ("isni", "0000000031304729")
    assert is_journal_published_by(journal, publisher, client=client)


@pytest.mark.nonpublic
def test_get_journal_for_publication():
    client = _get_client()
    publication = ("pubmed", "11818301")
    journals = get_journal_for_publication(publication, client=client)
    journal_list = list(journals)
    assert journal_list
    assert isinstance(journal_list[0], Node)
    assert journal_list[0].db_ns == "NLM"


@pytest.mark.nonpublic
def test_get_publications_for_journal():
    client = _get_client()
    journal = ("nlm", "100972832")
    publications = get_publications_for_journal(journal, client=client)
    publication_list = list(publications)
    assert publication_list
    assert isinstance(publication_list[0], Node)
    assert publication_list[0].db_ns == "PUBMED"


@pytest.mark.nonpublic
def test_is_published_in_journal():
    client = _get_client()
    publication = ("pubmed", "11818301")
    journal = ("nlm", "1254074")
    assert is_published_in_journal(publication, journal, client=client)

    # Test a relationship that shouldn't exist
    wrong_journal = ("nlm", "000000")
    assert not is_published_in_journal(publication, wrong_journal, client=client)


@pytest.mark.nonpublic
def test_get_diseases_for_gene():
    client = _get_client()
    gene = ("hgnc", "57")
    diseases = get_diseases_for_gene(gene, client=client)
    disease_list = list(diseases)
    assert disease_list
    assert isinstance(disease_list[0], Node)
    assert disease_list[0].db_ns in ["DOID", "MESH", "UMLS", "MONDO"]
    assert ("DOID", "DOID:2738") in [d.grounding() for d in disease_list]


@pytest.mark.nonpublic
def test_get_genes_for_disease():
    client = _get_client()
    disease = ("doid", "DOID:2738")
    genes = get_genes_for_disease(disease, client=client)
    gene_list = list(genes)
    assert gene_list
    assert isinstance(gene_list[0], Node)
    assert gene_list[0].db_ns == "HGNC"
    assert ("HGNC", "57") in [g.grounding() for g in gene_list]


@pytest.mark.nonpublic
def test_get_diseases_for_variant():
    client = _get_client()
    variant = ("dbsnp", "rs9994441")
    diseases = get_diseases_for_variant(variant, client=client)
    disease_list = list(diseases)
    assert disease_list
    assert isinstance(disease_list[0], Node)
    assert disease_list[0].db_ns in ["DOID", "UMLS"]
    assert ("DOID", "DOID:10652") in [d.grounding() for d in disease_list]


@pytest.mark.nonpublic
def test_get_variants_for_disease():
    client = _get_client()
    disease = ("doid", "DOID:10652")
    variants = get_variants_for_disease(disease, client=client)
    variant_list = list(variants)
    assert variant_list
    assert isinstance(variant_list[0], Node)
    assert variant_list[0].db_ns == "DBSNP"
    assert ("DBSNP", "rs9994441") in [v.grounding() for v in variant_list]


@pytest.mark.nonpublic
def test_get_genes_for_variant():
    client = _get_client()
    variant = ("dbsnp", "rs74615166")
    genes = get_genes_for_variant(variant, client=client)
    gene_list = list(genes)
    assert gene_list
    assert isinstance(gene_list[0], Node)
    assert gene_list[0].db_ns == "HGNC"
    assert ("HGNC", "12310") in [g.grounding() for g in gene_list]


@pytest.mark.nonpublic
def test_get_variants_for_gene():
    client = _get_client()
    gene = ("hgnc", "9896")
    variants = get_variants_for_gene(gene, client=client)
    variant_list = list(variants)
    assert variant_list
    assert isinstance(variant_list[0], Node)
    assert variant_list[0].db_ns == "DBSNP"


@pytest.mark.nonpublic
def test_has_gene_disease_association():
    client = _get_client()
    gene = ("hgnc", "57")
    disease = ("doid", "DOID:2738")
    assert has_gene_disease_association(gene, disease, client=client)


@pytest.mark.nonpublic
def test_has_variant_disease_association():
    client = _get_client()
    variant = ("dbsnp", "rs9994441")
    disease = ("doid", "DOID:10652")
    assert has_variant_disease_association(variant, disease, client=client)


@pytest.mark.nonpublic
def test_has_variant_gene_association():
    client = _get_client()
    variant = ("dbsnp", "rs74615166")
    gene = ("hgnc", "12310")
    assert has_variant_gene_association(variant, gene, client=client)


@pytest.mark.nonpublic
def test_get_publications_for_project():
    client = _get_client()
    project = ("nihreporter.project", "6439077")
    publications = get_publications_for_project(project, client=client)
    pub_list = list(publications)
    assert pub_list
    assert isinstance(pub_list[0], Node)
    assert pub_list[0].db_ns == "PUBMED"


@pytest.mark.nonpublic
def test_get_projects_for_publication():
    client = _get_client()
    publication = ("pubmed", "11818301")
    projects = get_projects_for_publication(publication, client=client)
    project_list = list(projects)
    assert project_list
    assert isinstance(project_list[0], Node)
    assert project_list[0].db_ns == "NIHREPORTER.PROJECT"
    assert ("NIHREPORTER.PROJECT", "2106659") in [p.grounding() for p in project_list]


@pytest.mark.nonpublic
def test_get_clinical_trials_for_project():
    client = _get_client()
    project = ("nihreporter.project", "6439077")
    trials = get_clinical_trials_for_project(project, client=client)
    trial_list = list(trials)
    assert trial_list
    assert isinstance(trial_list[0], Node)
    assert trial_list[0].db_ns == "CLINICALTRIALS"
    assert ("CLINICALTRIALS", "NCT00201240") in [t.grounding() for t in trial_list]


@pytest.mark.nonpublic
def test_get_projects_for_clinical_trial():
    client = _get_client()
    trial = ("clinicaltrials", "NCT00201240")
    projects = get_projects_for_clinical_trial(trial, client=client)
    project_list = list(projects)
    assert project_list
    assert isinstance(project_list[0], Node)
    assert project_list[0].db_ns == "NIHREPORTER.PROJECT"
    assert ("NIHREPORTER.PROJECT", "6439077") in [p.grounding() for p in project_list]


@pytest.mark.nonpublic
def test_get_patents_for_project():
    client = _get_client()
    project = ("nihreporter.project", "2106676")
    patents = get_patents_for_project(project, client=client)
    patent_list = list(patents)
    assert patent_list
    assert isinstance(patent_list[0], Node)
    assert patent_list[0].db_ns == "GOOGLE.PATENT"
    assert ("GOOGLE.PATENT", "US5939275") in [p.grounding() for p in patent_list]


@pytest.mark.nonpublic
def test_get_projects_for_patent():
    client = _get_client()
    patent = ("google.patent", "US5939275")
    projects = get_projects_for_patent(patent, client=client)
    project_list = list(projects)
    assert project_list
    assert isinstance(project_list[0], Node)
    assert project_list[0].db_ns == "NIHREPORTER.PROJECT"
    assert ("NIHREPORTER.PROJECT", "2106676") in [p.grounding() for p in project_list]


@pytest.mark.nonpublic
def test_get_domains_for_gene():
    client = _get_client()
    gene = ("hgnc", "475")
    domains = get_domains_for_gene(gene, client=client)
    domain_list = list(domains)
    assert domain_list
    assert isinstance(domain_list[0], Node)
    assert domain_list[0].db_ns == "IP"
    assert ("IP", "IPR006047") in [d.grounding() for d in domain_list]


@pytest.mark.nonpublic
def test_get_genes_for_domain():
    client = _get_client()
    domain = ("interpro", "IPR006047")
    genes = get_genes_for_domain(domain, client=client)
    gene_list = list(genes)
    assert gene_list
    assert isinstance(gene_list[0], Node)
    assert gene_list[0].db_ns == "HGNC"
    assert ("HGNC", "475") in [g.grounding() for g in gene_list]


@pytest.mark.nonpublic
def test_gene_has_domain():
    client = _get_client()
    gene = ("hgnc", "475")
    domain = ("interpro", "IPR006047")
    assert gene_has_domain(gene, domain, client=client)

    # Test a relationship that shouldn't exist
    wrong_domain = ("interpro", "IPR000000")
    assert not gene_has_domain(gene, wrong_domain, client=client)


@pytest.mark.nonpublic
def test_get_phenotypes_for_variant_gwas():
    client = _get_client()
    variant = ("dbsnp", "rs13015548")
    phenotypes = get_phenotypes_for_variant_gwas(variant, client=client)
    phenotype_list = list(phenotypes)
    assert phenotype_list
    assert isinstance(phenotype_list[0], Node)
    assert phenotype_list[0].db_ns in ["MESH", "EFO", "DOID"]
    assert ("MESH", "D001827") in [p.grounding() for p in phenotype_list]


@pytest.mark.nonpublic
def test_get_variants_for_phenotype_gwas():
    client = _get_client()
    phenotype = ("mesh", "D001827")
    variants = get_variants_for_phenotype_gwas(phenotype, client=client)
    variant_list = list(variants)
    assert variant_list
    assert isinstance(variant_list[0], Node)
    assert variant_list[0].db_ns == "DBSNP"
    assert ("DBSNP", "rs13015548") in [v.grounding() for v in variant_list]


@pytest.mark.nonpublic
def test_has_variant_phenotype_association():
    client = _get_client()
    variant = ("dbsnp", "rs13015548")
    phenotype = ("mesh", "D001827")
    assert has_variant_phenotype_association(variant, phenotype, client=client)

    # Test a relationship that shouldn't exist
    wrong_phenotype = ("mesh", "D000000")
    assert not has_variant_phenotype_association(variant, wrong_phenotype, client=client)


@pytest.mark.nonpublic
def test_get_indications_for_drug():
    client = _get_client()
    molecule = ("chebi", "10001")
    indications = get_indications_for_drug(molecule, client=client)
    indication_list = list(indications)
    assert indication_list
    assert isinstance(indication_list[0], Node)
    assert indication_list[0].db_ns == "MESH"
    assert ("MESH", "D002318") in [i.grounding() for i in indication_list]


@pytest.mark.nonpublic
def test_get_drugs_for_indication():
    client = _get_client()
    indication = ("mesh", "D002318")
    molecules = get_drugs_for_indication(indication, client=client)
    molecule_list = list(molecules)
    assert molecule_list
    assert isinstance(molecule_list[0], Node)
    assert molecule_list[0].db_ns in ["CHEBI", "CHEMBL"]


@pytest.mark.nonpublic
def test_drug_has_indication():
    client = _get_client()
    molecule = ("chebi", "10001")
    indication = ("mesh", "D002318")
    assert drug_has_indication(molecule, indication, client=client)

    # Test a relationship that shouldn't exist
    wrong_indication = ("mesh", "D000000")
    assert not drug_has_indication(molecule, wrong_indication, client=client)


@pytest.mark.nonpublic
def test_check_ec():
    client = _get_client()
    print("\nChecking EC relationships...")

    # Check format of has_activity relationship
    query = """
    MATCH (b1:BioEntity)-[r:has_activity]->(b2:BioEntity)
    RETURN DISTINCT b1.id, b2.id, b1.type, b2.type
    LIMIT 5
    """
    result = client.query_tx(query)
    print("\nFound EC relationships:", result)


@pytest.mark.nonpublic
def test_get_enzyme_activities_for_gene():
    client = _get_client()
    gene = ("hgnc", "10007")
    enzymes = get_enzyme_activities_for_gene(gene, client=client)
    enzyme_list = list(enzymes)
    assert enzyme_list
    assert isinstance(enzyme_list[0], Node)
    assert enzyme_list[0].db_ns == "ECCODE"
    assert ("ECCODE", "3.4.21.105") in [e.grounding() for e in enzyme_list]


@pytest.mark.nonpublic
def test_get_genes_for_enzyme_activity():
    client = _get_client()
    enzyme = ("ec-code", "3.4.21.105")
    genes = get_genes_for_enzyme_activity(enzyme, client=client)
    gene_list = list(genes)
    assert gene_list
    assert isinstance(gene_list[0], Node)
    assert gene_list[0].db_ns == "HGNC"
    assert ("HGNC", "10007") in [g.grounding() for g in gene_list]


@pytest.mark.nonpublic
def test_has_enzyme_activity():
    client = _get_client()
    gene = ("hgnc", "10007")
    enzyme = ("ec-code", "3.4.21.105")
    assert has_enzyme_activity(gene, enzyme, client=client)

    # Test a relationship that shouldn't exist
    wrong_enzyme = ("ec-code", "1.1.1.1")
    assert not has_enzyme_activity(gene, wrong_enzyme, client=client)


@pytest.mark.nonpublic
def test_get_cell_lines_with_mutation():
    client = _get_client()
    gene = ("hgnc", "11504")  # Gene we know exists from the check
    cell_lines = get_cell_lines_with_mutation(gene, client=client)
    cell_line_list = list(cell_lines)
    assert cell_line_list
    assert isinstance(cell_line_list[0], Node)
    assert cell_line_list[0].db_ns == "CCLE"
    assert ("CCLE", "HEL_HAEMATOPOIETIC_AND_LYMPHOID_TISSUE") in [c.grounding() for c in cell_line_list]


@pytest.mark.nonpublic
def test_get_mutated_genes_in_cell_line():
    client = _get_client()
    cell_line = ("ccle", "HEL_HAEMATOPOIETIC_AND_LYMPHOID_TISSUE")
    genes = get_mutated_genes_in_cell_line(cell_line, client=client)
    gene_list = list(genes)
    assert gene_list
    assert isinstance(gene_list[0], Node)
    assert gene_list[0].db_ns == "HGNC"
    assert ("HGNC", "11504") in [g.grounding() for g in gene_list]


@pytest.mark.nonpublic
def test_is_gene_mutated_in_cell_line():
    client = _get_client()
    gene = ("hgnc", "11504")
    cell_line = ("ccle", "HEL_HAEMATOPOIETIC_AND_LYMPHOID_TISSUE")
    assert is_gene_mutated_in_cell_line(gene, cell_line, client=client)

    # Test a relationship that shouldn't exist
    wrong_cell_line = ("ccle", "NONEXISTENT_CELL_LINE")
    assert not is_gene_mutated_in_cell_line(gene, wrong_cell_line, client=client)


@pytest.mark.nonpublic
def test_get_cell_lines_with_cna():
    client = _get_client()
    gene = ("hgnc", "11216")
    cell_lines = get_cell_lines_with_cna(gene, client=client)
    cell_line_list = list(cell_lines)
    assert cell_line_list
    assert isinstance(cell_line_list[0], Node)
    assert cell_line_list[0].db_ns == "CCLE"
    assert ("CCLE", "U266B1_HAEMATOPOIETIC_AND_LYMPHOID_TISSUE") in [c.grounding() for c in cell_line_list]


@pytest.mark.nonpublic
def test_get_cna_genes_in_cell_line():
    client = _get_client()
    cell_line = ("ccle", "HEL_HAEMATOPOIETIC_AND_LYMPHOID_TISSUE")
    genes = get_cna_genes_in_cell_line(cell_line, client=client)
    gene_list = list(genes)
    assert gene_list
    assert isinstance(gene_list[0], Node)
    assert gene_list[0].db_ns == "HGNC"


@pytest.mark.nonpublic
def test_has_cna_in_cell_line():
    client = _get_client()
    gene = ("hgnc", "11216")
    cell_line = ("ccle", "U266B1_HAEMATOPOIETIC_AND_LYMPHOID_TISSUE")
    assert has_cna_in_cell_line(gene, cell_line, client=client)

    # Test a relationship that shouldn't exist
    wrong_cell_line = ("ccle", "NONEXISTENT_CELL_LINE")
    assert not has_cna_in_cell_line(gene, wrong_cell_line, client=client)


@pytest.mark.nonpublic
def test_get_drugs_for_sensitive_cell_line():
    client = _get_client()
    cell_line = ("ccle", "HEL_HAEMATOPOIETIC_AND_LYMPHOID_TISSUE")
    drugs = get_drugs_for_sensitive_cell_line(cell_line, client=client)
    drug_list = list(drugs)
    assert drug_list
    assert isinstance(drug_list[0], Node)
    assert drug_list[0].db_ns in ["MESH", "CHEBI"]
    assert ("MESH", "C586365") in [d.grounding() for d in drug_list]


@pytest.mark.nonpublic
def test_get_sensitive_cell_lines_for_drug():
    client = _get_client()
    drug = ("mesh", "C586365")
    cell_lines = get_sensitive_cell_lines_for_drug(drug, client=client)
    cell_line_list = list(cell_lines)
    assert cell_line_list
    assert isinstance(cell_line_list[0], Node)
    assert cell_line_list[0].db_ns == "CCLE"
    assert ("CCLE", "RL952_ENDOMETRIUM") in [c.grounding() for c in cell_line_list]


@pytest.mark.nonpublic
def test_is_cell_line_sensitive_to_drug():
    client = _get_client()
    cell_line = ("ccle", "HEL_HAEMATOPOIETIC_AND_LYMPHOID_TISSUE")
    drug = ("mesh", "C586365")
    assert is_cell_line_sensitive_to_drug(cell_line, drug, client=client)

    # Test a relationship that shouldn't exist
    wrong_drug = ("mesh", "C000000")
    assert not is_cell_line_sensitive_to_drug(cell_line, wrong_drug, client=client)
