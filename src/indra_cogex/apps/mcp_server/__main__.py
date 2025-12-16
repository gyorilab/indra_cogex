"""MCP server entry point for INDRA CoGEx.

Run with: python -m indra_cogex.apps.mcp_server

This starts the MCP server using stdio transport, compatible with any MCP client
(Claude Desktop, Claude Code, Cursor, Zed, or custom integrations).

Neo4j credentials are read from environment variables:
  - INDRA_NEO4J_URL
  - INDRA_NEO4J_USER
  - INDRA_NEO4J_PASSWORD
"""
from .server import mcp

if __name__ == "__main__":
    mcp.run()
