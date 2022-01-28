"""Tests for the autoclient decorator."""

import pytest

from indra_cogex.client.neo4j_client import autoclient
from indra_cogex.representation import Node


def test_missing():
    """Test failure when missing the "client" argument."""
    with pytest.raises(ValueError):
        @autoclient
        def func(something_else):
            pass


def test_positional_exception():
    """Test failure on a positional argument."""
    with pytest.raises(ValueError):
        @autoclient
        def func(client):
            pass


def test_autoclient():
    """Test failure on a positional argument."""

    @autoclient
    def get_tissues_for_gene(gene, *, client):
        return client.get_targets(
            gene,
            relation="expressed_in",
            source_type="BioEntity",
            target_type="BioEntity",
        )

    tissues = get_tissues_for_gene(("HGNC", "9896"))
    assert tissues
    node0 = tissues[0]
    assert isinstance(node0, Node)
    assert node0.db_ns in {"UBERON", "CL"}
    assert ("UBERON", "UBERON:0002349") in {g.grounding() for g in tissues}
