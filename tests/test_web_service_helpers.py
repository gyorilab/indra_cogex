"""
Tests functionalities related to the CoGEx web service serving INDRA Discovery
"""
import json

from indra.statements import Evidence, Agent, Activation
from indra_cogex.apps.utils import _stmt_to_row


def test__stmt_to_row():
    db_ev = Evidence._from_json(
        {
            "source_api": "biopax",
            "pmid": "12917261",
            "source_id": "http://pathwaycommons.org/pc12/Catalysis_8049495032c7bba740de082d7bf6c3da",
            "annotations": {"source_sub_id": "pid"},
            "epistemics": {"direct": True},
            "text_refs": {"PMID": "12917261"},
            "source_hash": 7478359958559662154,
        }
    )
    a = Agent("a")
    b = Agent("b")
    db_stmt = Activation(a, b, evidence=[db_ev])
    source_counts = {"biopax": 1}
    ev_array, english, stmt_hash, sources, total_evidence, badges, = _stmt_to_row(
        stmt=db_stmt,
        cur_dict={},
        cur_counts={},
        source_counts=source_counts,
        include_belief_badge=True,
    )
    assert int(total_evidence) == 1
    assert "biopax" in ev_array
    assert 'null' in ev_array
    assert sources == json.dumps(source_counts)
    assert english == '"<b>A</b> activates <b>b</b>."'


def test__stmt_to_row_medscan():
    db_ev_list = [
        Evidence._from_json(
            {
                "source_api": "medscan",
                "pmid": "12345678",
                "text_refs": {"PMID": "12345678"},
                "source_hash": 1234567890123456789,
            }
        ),
        Evidence._from_json(
            {
                "source_api": "signor",
                "pmid": "12345679",
                "text_refs": {"PMID": "12345679"},
                "source_hash": 1234567890123456790,
            }
        )
    ]
    a = Agent("x")
    b = Agent("y")
    db_stmt = Activation(a, b, evidence=db_ev_list)
    source_counts = {"medscan": 1, "signor": 1}
    (
        ev_array,
        english,
        stmt_hash,
        sources,
        total_evidence,
        badges,
    ) = _stmt_to_row(
        stmt=db_stmt,
        cur_dict={},
        cur_counts={},
        source_counts=source_counts,
        include_belief_badge=True,
    )
    assert int(total_evidence) == 1
    assert stmt_hash is not None
    assert "signor" in ev_array
    assert "medscan" not in ev_array
    assert sources == json.dumps(source_counts)
    assert english == '"<b>X</b> activates <b>y</b>."'
    assert sources == '{"signor": 1}'
    assert '"num": 1,' in badges  # Evidence count badge
