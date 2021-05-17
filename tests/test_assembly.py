from indra_cogex.assembly import NodeAssembler
from indra_cogex.representation import Node


def test_add_nodes():
    na = NodeAssembler([Node("x", "y", ["l"])])
    assert len(na.nodes) == 1
    na.add_nodes([Node("y", "z", ["l"])])
    assert len(na.nodes) == 2


def test_merge_properties():
    n1 = Node("ns", "id", ["l"], {"k1": "v1"})
    n2 = Node("ns", "id", ["l"], {"k2": "v2"})
    na = NodeAssembler([n1, n2])
    ans = na.assemble_nodes()
    assert len(ans) == 1
    assert ans[0].data == {"k1": "v1", "k2": "v2"}


def test_merge_labels():
    n1 = Node("ns", "id", ["l1", "l2"])
    n2 = Node("ns", "id", ["l2", "l3"])
    na = NodeAssembler([n1, n2])
    ans = na.assemble_nodes()
    assert len(ans) == 1
    assert set(ans[0].labels) == {"l1", "l2", "l3"}


def test_merge_conflict():
    n1 = Node("ns", "id", ["l"], {"k1": "v1"})
    n2 = Node("ns", "id", ["l"], {"k1": "v2"})
    na = NodeAssembler([n1, n2])
    ans = na.assemble_nodes()
    assert len(ans) == 1
    assert ans[0].data == {"k1": "v1"}
    assert len(na.conflicts) == 1
    assert na.conflicts[0].key == "k1"
    assert na.conflicts[0].val1 == "v1"
    assert na.conflicts[0].val2 == "v2"
