"""Tests for the autoclient decorator."""

from collections import Counter
from typing import Tuple

import pytest

from indra_cogex.client.neo4j_client import Neo4jClient, autoclient
from indra_cogex.representation import Node


def test_missing():
    """Test failure when missing the "client" argument."""
    with pytest.raises(ValueError):
        @autoclient()
        def func(something_else):
            pass


def test_positional_exception():
    """Test failure on a positional argument."""
    with pytest.raises(ValueError):
        @autoclient()
        def func(client):
            pass


def test_autoclient():
    """Test a successful application of the autoclient decorator."""

    @autoclient()
    def get_tissues_for_gene(gene: Tuple[str, str], *, client: Neo4jClient):
        return client.get_targets(
            gene,
            relation="expressed_in",
            source_type="BioEntity",
            target_type="BioEntity",
        )

    assert not hasattr(get_tissues_for_gene, "cache_info"), "caching is not enabled"

    tissues = get_tissues_for_gene(("HGNC", "9896"))
    assert tissues
    node0 = tissues[0]
    assert isinstance(node0, Node)
    assert node0.db_ns in {"UBERON", "CL"}
    assert ("UBERON", "UBERON:0002349") in {g.grounding() for g in tissues}


def test_autoclient_cached():
    """Test caching a function with autoclient."""

    @autoclient(cache=True)
    def get_node_count(*, client: Neo4jClient) -> Counter:
        return Counter(
            {
                label[0]: client.query_tx(f"MATCH (n:{label[0]}) RETURN count(*)")[0][0]
                for label in client.query_tx("call db.labels();")
            }
        )

    assert hasattr(get_node_count, "cache_info"), "caching should be enabled"
    node_counts = get_node_count()
    assert isinstance(node_counts, Counter)
    assert "BioEntity" in node_counts
    assert all(isinstance(key, str) for key in node_counts.keys())
    assert all(isinstance(key, int) for key in node_counts.values())
