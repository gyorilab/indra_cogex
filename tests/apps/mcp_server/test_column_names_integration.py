"""Integration test for column name preservation in query results.

This test verifies that Cypher query column names from RETURN clauses
are preserved instead of being replaced with generic col_0, col_1, etc.

Run with: pytest -m nonpublic tests/apps/mcp_server/test_column_names_integration.py
"""
import asyncio
import pytest
from flask import Flask
from indra.config import get_config
from indra_cogex.client.neo4j_client import Neo4jClient
from indra_cogex.apps.constants import INDRA_COGEX_EXTENSION
from indra_cogex.apps.mcp_server.query_execution import execute_cypher


