import pytest
from indra_cogex.client.queries import *
from indra.statements import *

from .test_neo4j_client import _get_client


@pytest.mark.nonpublic
def test_get_pmids_for_mesh():
    # Single query
    client = _get_client()
    pmids = get_pmids_for_mesh(client, ('MESH', 'D015002'))
    assert len(pmids) == 1591
    assert pmids[0].startswith('pubmed:')


@pytest.mark.nonpublic
def test_get_mesh_ids_for_pmid():
    # Single query
    client = _get_client()
    pmid = ("PUBMED", "27890007")
    mesh_ids = get_mesh_ids_for_pmid(client, pmid)
    assert len(mesh_ids) == 4
    assert mesh_ids[0].startswith('mesh:')


@pytest.mark.nonpublic
def test_get_evidence_obj_for_stmt_hash():
    # Note: This statement has a ~500 evidences
    # Single query
    stmt_hash = '35279776755000170'
    client = _get_client()
    ev_objs = get_evidence_obj_for_stmt_hash(client, stmt_hash)
    assert len(ev_objs) == 529
    assert isinstance(ev_objs[0], Evidence)


@pytest.mark.nonpublic
def test_get_evidence_obj_for_stmt_hashes():
    # Note: This statement has ~500 of evidences
    # Single query
    stmt_hashes = ['35279776755000170']
    client = _get_client()
    ev_dict = get_evidence_obj_for_stmt_hashes(client, stmt_hashes)
    assert len(ev_dict) == 1
    assert list(ev_dict.keys())[0] == '35279776755000170'
    assert len(ev_dict['35279776755000170']) == 529
    assert isinstance(ev_dict['35279776755000170'][0], Evidence)


@pytest.mark.nonpublic
def test_get_stmts_for_pmid():
    # Two queries: first evidences, then the statements
    client = _get_client()
    pmid = ("PUBMED", "14898026")
    stmts = get_stmts_for_pmid(client, pmid)
    assert len(stmts) == 1
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
    assert len(stmts) == 1
    assert isinstance(stmts[0], Activation)


@pytest.mark.nonpublic
def test_get_stmts_by_hashes():
    # Note: This statement has a ~500 of evidences
    # Two queries: first statements, then all the evidence for the statements
    stmt_hashes = ['35279776755000170']
    client = _get_client()
    stmts = get_stmts_for_stmt_hashes(client, stmt_hashes)
    assert len(stmts) == 1
    assert isinstance(stmts[0], Inhibition)
