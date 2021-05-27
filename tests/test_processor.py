from indra_cogex.representation import norm_id


def test_norm_id():
    assert norm_id("UP", "P12345") == "uniprot:P12345"
    assert norm_id("CHEBI", "CHEBI:12345") == "chebi:12345"
