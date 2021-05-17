from indra_cogex.sources.indra_db import fix_id


def test_fix_id():
    assert fix_id("EFO", "EFO:12345") == ("EFO", "12345")
    assert fix_id("GO", "123") == ("GO", "GO:0000123")
    assert fix_id("CHEBI", "123") == ("CHEBI", "CHEBI:123")
    assert fix_id("UP", "P12345-6") == ("UP", "P12345")
    assert fix_id("UP", "SL-123") == ("UPLOC", "SL-123")
