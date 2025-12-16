"""Pytest configuration for MCP server integration tests.

Provides MCP-specific fixtures for Neo4j client and Flask app context.
Environment variables are loaded by root tests/conftest.py.
"""
import pytest
from flask import Flask
from indra.config import get_config
from indra_cogex.client.neo4j_client import Neo4jClient
from indra_cogex.apps.constants import INDRA_COGEX_EXTENSION


# Shared fixtures for all MCP server tests
@pytest.fixture
def neo4j_client():
    """Provide real Neo4j client for tests."""
    return Neo4jClient(
        get_config("INDRA_NEO4J_URL"),
        auth=(get_config("INDRA_NEO4J_USER"), get_config("INDRA_NEO4J_PASSWORD"))
    )


@pytest.fixture
def flask_app_with_client(neo4j_client):
    """Flask app context with real Neo4j client."""
    app = Flask(__name__)
    app.config["SECRET_KEY"] = "test-key"
    app.extensions[INDRA_COGEX_EXTENSION] = neo4j_client
    with app.app_context():
        yield app
