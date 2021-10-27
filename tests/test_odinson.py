from indra_cogex.sources.odinson.document import grounded_agents_from_tokens, Token


def test_get_agents_from_tokens():
    tokens = [
        Token("a", "a", "DT", "a", "O", "O"),
        Token("breast", "breast", "NN", "breast", "B-TissueType", "O"),
        Token("cancer", "cancer", "NN", "cancer", "I-TissueType", "O"),
        Token("is", "is", "VBZ", "be", "O", "O"),
        Token("a", "a", "DT", "a", "O", "O"),
        Token("tumor", "tumor", "NN", "tumor", "B-TissueType", "O"),
        Token("burden", "burden", "NN", "burden", "I-TissueType", "O"),
    ]
    agents = grounded_agents_from_tokens(tokens)
    assert len(agents) == 2
    assert agents[0].name == "Breast Neoplasms"
    assert agents[0].db_refs == {"MESH": "D001943"}
