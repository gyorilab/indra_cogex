import json

import pytest

from indra.statements import stmt_from_json
from indra.tools import assemble_corpus as ac
from indra_cogex.util import load_stmt_json_str


def test_escaped_unicode():
    """Test that doubly escaped unicode is handled correctly."""
    source_hash = 8921534277374933489
    sjs = (
        '{"type": "Complex", "members": [{"name": "PPP1CA", "db_refs": {'
        '"UP": "P62136", "TEXT": "PP1\\u03b1", "HGNC": "9281"}}, '
        '{"name": "PPP1", "db_refs": {"TEXT": "PP1", "NXPFA": "03001", '
        '"FPLX": "PPP1"}}], "belief": 1.0, "evidence": [{"source_api": '
        '"sparser", "text": "These results suggest that multiple PC1 '
        'sites are involved in PP1\\u03b1 binding and that PP1\\u03b1 '
        'interacts with the conserved PP1-binding motif plus additional '
        'elements within the membrane distal portion of the PC1 '
        'C-tail.", "annotations": {"found_by": "INTERACT"}, "text_refs": '
        '{"PMID": "PMC18307576"}, "source_hash": 8921534277374933489}], '
        '"id": "eaf7529d-fd65-45b7-86ff-84dbeb764550"}'
    )
    sj = load_stmt_json_str(sjs)
    stmt = stmt_from_json(sj)
    assert stmt.evidence[0].source_hash == source_hash

    # Check that the statement survives a round trip to json.dumps
    sjs2 = json.dumps(stmt.to_json())
    sj3 = load_stmt_json_str(sjs2)
    stmt3 = stmt_from_json(sj3)
    assert stmt3.evidence[0].source_hash == source_hash


def test_quadruple_escaped_chemical_name_doubly_escaped_unicode():
    matches_hash = 16637653806582621
    sjs = (
        '{"type": "Activation", "subj": {"name": "N-[2-hydroxy-5-('
        '1-hydroxy-2-\\\\{[1-('
        '4-methoxyphenyl)propan-2-yl]amino\\\\}ethyl)phenyl'
        ']formamide", "db_refs": {"CHEBI": "CHEBI:63082", "HMDB": '
        '"HMDB0015118", "PUBCHEM": "3410", "DRUGBANK": "DB00983", "CHEMBL": '
        '"CHEMBL1256786", "CAS": "73573-87-2"}}, "obj": {"name": "ADRB2", '
        '"db_refs": {"UP": "P07550", "HGNC": "286", "EGID": "154"}}, '
        '"obj_activity": "activity", "belief": 1, "evidence": [{'
        '"source_api": "signor", "pmid": "20590599", "source_id": '
        '"SIGNOR-257853", "text": "Thus, overall, salmeterol is a highly '
        'selective \\u03b22-adrenoceptor agonist because of its higher '
        '\\u03b22-affinity and not because of higher \\u03b22-intrinsic '
        'efficacy. A similar reasoning can be applied to formoterol, although '
        'this agonist has higher intrinsic efficacy at all three receptors '
        '(rank 6, 8 and 5 at \\u03b21, \\u03b22\\u00a0and \\u03b23).", '
        '"annotations": {"SEQUENCE": null, "MODULATOR_COMPLEX": null, '
        '"TARGET_COMPLEX": null, "MODIFICATIONA": null, "MODASEQ": null, '
        '"MODIFICATIONB": null, "MODBSEQ": null, "NOTES": null, "ANNOTATOR": '
        '"Luana"}, "epistemics": {"direct": true}, "context": {"cell_type": '
        '{"name": null, "db_refs": {"BTO": "BTO:0000457"}}, "species": '
        '{"name": null, "db_refs": {"TAXONOMY": "10030"}}, "type": "bio"}, '
        '"text_refs": {"PMID": "20590599"}, "source_hash": '
        '-4455644815662527647}], "id": '
        '"4697a750-f01c-4d06-80b7-416143e33dd1", "matches_hash": '
        '"16637653806582621"}'
    )
    sj = load_stmt_json_str(sjs)
    stmt = stmt_from_json(sj)
    assert stmt.evidence[0].source_hash == -4455644815662527647
    assert stmt.get_hash(refresh=True) == matches_hash

    # Check that the statement survives a round trip to json.dumps
    sjs2 = json.dumps(stmt.to_json())
    sj3 = load_stmt_json_str(sjs2)
    stmt3 = stmt_from_json(sj3)
    assert stmt3.evidence[0].source_hash == -4455644815662527647
    assert stmt3.get_hash(refresh=True) == matches_hash


def test_quad_escaped_unicode():
    sjs = (
        '{"type": "Inhibition", "subj": {"name": "\\\\u0394", "db_refs": {'
        '"TEXT": "\\\\u0394"}}, "obj": {"name": "Infections", "db_refs": {'
        '"MESH": "D007239", "TEXT": "infection", "EFO": "0000544"}}, '
        '"obj_activity": "activity", "belief": 1, "evidence": [{'
        '"source_api": "reach", "text": "A previous study demonstrated that '
        'Syn61\\\\u03943 resists infection by multiple bacteriophages, '
        'including Enterobacteria phage T6  .", "annotations": {"found_by": '
        '"Negative_activation_syntax_1_verb", "agents": {"coords": [[40, '
        '41], [51, 60]]}}, "epistemics": {"direct": false, "section_type": '
        'null}, "text_refs": {"PMID": "78437624"}, "source_hash": '
        '-803868470175671675}], "id": '
        '"0652bc92-7078-4c46-989e-b1a0bebbe348", "matches_hash": '
        '"-24102351504334505"}'
    )
    sj = load_stmt_json_str(sjs)
    stmt = stmt_from_json(sj)
    assert stmt.evidence[0].source_hash == -803868470175671675
    assert stmt.get_hash(refresh=True) == -24102351504334505

    # Check that the statement survives a round trip to json.dumps
    sjs2 = json.dumps(stmt.to_json())
    sj3 = load_stmt_json_str(sjs2)
    stmt3 = stmt_from_json(sj3)
    assert stmt3.evidence[0].source_hash == -803868470175671675
    assert stmt3.get_hash(refresh=True) == -24102351504334505


@pytest.mark.slow
def test_escaped_db_refs_grounding_mapping():
    sjs = (
        '{"type": "Activation", "subj": {"name": "TGFB1", "db_refs": {'
        '"TEXT": "TGF-\\\\u03b21"}}, "obj": {"name": "NOX4", "db_refs": {'
        '"HGNC": "7891", "UP": "Q9NPH5", "TEXT": "Nox4"}}, "obj_activity": '
        '"activity", "belief": 1, "evidence": [{"source_api": "medscan", '
        '"pmid": "28063381", "source_id": "info:pmid/28063381", "text": '
        '"Moreover, Nox4, which is constitutively active in renal cells and '
        'is involvedin the generation of hydrogen peroxide, was up-regulated '
        'during ureteral obstruction-mediated fibrosis and induced by '
        'TGF-\\\\u03b21 in HK-2 cells, and this up-regulation could be '
        'blunted by Brd4 inhibition.", "annotations": {"verb": '
        '"UnknownRegulation-positive", "last_verb": "TK{induce}", "agents": '
        '{"coords": [[196, 202], [10, 14]]}}, "epistemics": {"direct": '
        'false}, "text_refs": {"PMID": "28063381"}, "source_hash": '
        '4793198277843896406}], "id": "66d48a98-12d4-4a68-8485-cc57d37f677e"}'
    )
    sj = load_stmt_json_str(sjs)
    stmt = stmt_from_json(sj)

    # Check that the statement survives a round trip to json.dumps
    sjs2 = json.dumps(stmt.to_json())
    sj2 = json.loads(sjs2)
    stmt2 = stmt_from_json(sj2)
    assert stmt2.get_hash(refresh=True) == stmt.get_hash(refresh=True)

    # Check that the cleaning allows for grounding mapping
    unesc_sj = json.loads(sjs)
    unesc_stmt = stmt_from_json(unesc_sj)
    unesc_stmts = ac.fix_invalidities([unesc_stmt], in_place=True)
    unesc_stmts = ac.map_grounding(unesc_stmts)
    mapped_unesc_stmt = ac.map_sequence(unesc_stmts)[0]
    unesc_subj_db_refs = mapped_unesc_stmt.subj.db_refs

    esc_stmt = stmt
    esc_stmts = ac.fix_invalidities([esc_stmt], in_place=True)
    esc_stmts = ac.map_grounding(esc_stmts)
    mapped_esc_stmt = ac.map_sequence(esc_stmts)[0]
    esc_subj_db_refs = mapped_esc_stmt.subj.db_refs

    # Relies on that the assemble_corpus pipeline doesn't fix the escaped
    # characters
    assert unesc_subj_db_refs != esc_subj_db_refs
