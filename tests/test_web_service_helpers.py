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
    assert "7478359958559662154" in ev_array
    assert 'null' in ev_array
    assert sources == json.dumps(source_counts)
    assert english == '"<b>A</b> activates <b>b</b>."'
