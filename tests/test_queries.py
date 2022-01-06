import pytest
from indra_cogex.client.queries import *
from indra.statements import Inhibition

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
    client = _get_client()
    pmid = ("PUBMED", "27890007")
    mesh_ids = get_mesh_ids_for_pmid(client, pmid)
    assert len(mesh_ids) == 4
    assert mesh_ids[0].startswith('mesh:')


@pytest.mark.nonpublic
def test_get_stmts_by_hashes():
    # Note This statement has a 100s of evidences
    # Two queries: first statements, then all the evidence for the statements
    stmt_hashes = ['35279776755000170']
    client = _get_client()
    stmts = get_stmts_for_stmt_hashes(client, stmt_hashes)
    assert len(stmts) == 1
    assert isinstance(stmts[0], Inhibition)
