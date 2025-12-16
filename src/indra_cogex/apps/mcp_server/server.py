"""MCP server with progressive disclosure pattern for knowledge graph navigation.

Architecture:
- Layer 1: Schema Discovery (get_graph_schema)
- Layer 2: Query Execution (execute_cypher)
- Layer 3: Validation (validate_cypher)
- Layer 4: Enrichment (enrich_results)
- Layer 5: Gateway Tools (4 tools for graph navigation):
    - ground_entity: Natural language → CURIEs via GILDA
    - suggest_endpoints: Graph navigation suggestions
    - call_endpoint: Execute any autoclient function
    - get_navigation_schema: Full edge map for discovery
"""
import asyncio
import json
import logging
import threading
from typing import Any, Dict, List, Optional

from mcp.server.fastmcp import FastMCP
from pydantic import BaseModel, Field, ConfigDict

from indra_cogex.client.neo4j_client import Neo4jClient
from indra.config import get_config
from .enrichment import enrich_results, DisclosureLevel
from .schema_discovery import get_graph_schema
from .query_execution import execute_cypher
from .validation import validate_cypher

logger = logging.getLogger(__name__)

# Initialize MCP server
mcp = FastMCP("indra_cogex")

# Neo4j client singleton
_neo4j_client = None
_client_lock = threading.RLock()


def get_client() -> Neo4jClient:
    """Get or create singleton Neo4j client (thread-safe)."""
    global _neo4j_client
    with _client_lock:
        if _neo4j_client is None:
            _neo4j_client = Neo4jClient(
                get_config("INDRA_NEO4J_URL"),
                auth=(get_config("INDRA_NEO4J_USER"), get_config("INDRA_NEO4J_PASSWORD"))
            )
            logger.info(f"Initialized Neo4j client: {get_config('INDRA_NEO4J_URL')}")
        return _neo4j_client


# ============================================================================
# Input Models
# ============================================================================

class GetGraphSchemaInput(BaseModel):
    """Input model for get_graph_schema."""
    model_config = ConfigDict(validate_assignment=True, extra='forbid')

    detail_level: str = Field(
        "summary",
        description="Schema detail level: summary, entity_types, relationship_types, patterns, full"
    )
    entity_type: Optional[str] = Field(
        None,
        description="Optional: Filter to specific entity type (e.g., 'Gene', 'Disease')"
    )
    relationship_type: Optional[str] = Field(
        None,
        description="Optional: Filter to specific relationship type"
    )


class ExecuteCypherInput(BaseModel):
    """Input model for execute_cypher."""
    model_config = ConfigDict(validate_assignment=True, extra='forbid')

    query: str = Field(..., description="Cypher query to execute")
    parameters: Dict[str, Any] = Field(
        default_factory=dict,
        description="Parameterized query values (prevents injection)"
    )
    validate_first: bool = Field(True, description="Run validation before execution")
    timeout_ms: int = Field(30000, ge=1000, le=120000, description="Query timeout in milliseconds")
    max_results: int = Field(100, ge=1, le=10000, description="Maximum result rows")
    explain: bool = Field(False, description="Return query plan instead of executing")
    offset: int = Field(0, ge=0, description="Starting offset for pagination (default: 0)")
    limit: Optional[int] = Field(
        None,
        ge=1,
        le=10000,
        description="Maximum items per response page. Auto-truncates to ~20k tokens if exceeded."
    )


class ValidateCypherInput(BaseModel):
    """Input model for validate_cypher."""
    model_config = ConfigDict(validate_assignment=True, extra='forbid')

    query: str = Field(..., description="Cypher query to validate")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Query parameters")


class EnrichResultsInput(BaseModel):
    """Input model for enrich_results."""
    model_config = ConfigDict(validate_assignment=True, extra='forbid')

    results: List[Any] = Field(..., description="Query results to enrich")
    disclosure_level: str = Field(
        "standard",
        description="Metadata level: minimal, standard, detailed, exploratory"
    )
    result_type: Optional[str] = Field(
        None,
        description="Type of results (e.g., 'gene', 'pathway')"
    )
    offset: int = Field(0, ge=0, description="Starting offset for pagination (default: 0)")
    limit: Optional[int] = Field(
        None,
        ge=1,
        le=10000,
        description="Maximum items per response page. Auto-truncates to ~20k tokens if exceeded."
    )


# ============================================================================
# Layer 1: Schema Discovery
# ============================================================================

@mcp.tool(
    name="get_graph_schema",
    annotations={"title": "Get Graph Schema", "readOnlyHint": True, "idempotentHint": True}
)
async def get_graph_schema_tool(params: GetGraphSchemaInput) -> str:
    """Progressively discover Neo4j graph schema (entity types, relationships).

    Returns
    -------
    :
        JSON with schema metadata at requested detail level
    """
    try:
        # Add 60 second timeout to prevent hangs on expensive queries
        result = await asyncio.wait_for(
            asyncio.to_thread(
                get_graph_schema,
                client=get_client(),
                detail_level=params.detail_level,
                entity_type=params.entity_type,
                relationship_type=params.relationship_type
            ),
            timeout=60.0
        )
        return json.dumps(result, indent=2)
    except asyncio.TimeoutError:
        logger.error("Schema discovery timed out after 60 seconds")
        return json.dumps({
            "error": "Schema discovery timed out after 60 seconds",
            "hint": "Try a simpler detail_level like 'summary' or filter by entity_type/relationship_type"
        }, indent=2)
    except Exception as e:
        logger.error(f"Schema discovery failed: {e}", exc_info=True)
        return json.dumps({"error": f"Schema discovery failed: {str(e)}"}, indent=2)


# ============================================================================
# Layer 2: Query Execution
# ============================================================================

@mcp.tool(
    name="execute_cypher",
    annotations={"title": "Execute Cypher Query", "readOnlyHint": True, "idempotentHint": True}
)
async def execute_cypher_tool(params: ExecuteCypherInput) -> str:
    """Execute arbitrary Cypher query on CoGEx knowledge graph.

    Returns
    -------
    :
        JSON with query results or error information
    """
    try:
        # Validate query if requested
        if params.validate_first:
            validation = await asyncio.to_thread(
                validate_cypher,
                query=params.query,
                parameters=params.parameters
            )
            if not validation.get("valid", False):
                return json.dumps({
                    "error": "Query validation failed",
                    "validation": validation
                }, indent=2)

        # Execute query with pagination
        result = await asyncio.to_thread(
            execute_cypher,
            client=get_client(),
            query=params.query,
            parameters=params.parameters,
            timeout_ms=params.timeout_ms,
            max_results=params.max_results,
            explain=params.explain,
            offset=params.offset,
            limit=params.limit,
        )
        return json.dumps(result, indent=2)

    except Exception as e:
        logger.error(f"Query execution failed: {e}", exc_info=True)
        return json.dumps({"error": f"Query execution failed: {str(e)}"}, indent=2)


# ============================================================================
# Layer 3: Validation
# ============================================================================

@mcp.tool(
    name="validate_cypher",
    annotations={"title": "Validate Cypher Query", "readOnlyHint": True, "idempotentHint": True}
)
async def validate_cypher_tool(params: ValidateCypherInput) -> str:
    """Validate Cypher query safety before execution.

    Returns
    -------
    :
        JSON with validation results
    """
    try:
        result = await asyncio.to_thread(
            validate_cypher,
            query=params.query,
            parameters=params.parameters
        )
        return json.dumps(result, indent=2)
    except Exception as e:
        logger.error(f"Query validation failed: {e}", exc_info=True)
        return json.dumps({"error": f"Validation failed: {str(e)}"}, indent=2)


# ============================================================================
# Layer 4: Result Enrichment
# ============================================================================

@mcp.tool(
    name="enrich_results",
    annotations={"title": "Enrich Query Results", "readOnlyHint": True, "idempotentHint": True}
)
async def enrich_results_tool(params: EnrichResultsInput) -> str:
    """Add progressive metadata to query results.

    Returns
    -------
    :
        JSON with enriched results
    """
    try:
        try:
            disclosure_level = DisclosureLevel(params.disclosure_level)
        except ValueError:
            return json.dumps({
                "error": f"Invalid disclosure_level '{params.disclosure_level}'",
                "valid_levels": ["minimal", "standard", "detailed", "exploratory"]
            }, indent=2)

        result = await asyncio.to_thread(
            enrich_results,
            results=params.results,
            disclosure_level=disclosure_level,
            result_type=params.result_type,
            client=get_client(),
            offset=params.offset,
            limit=params.limit,
        )
        return json.dumps(result, indent=2)

    except Exception as e:
        logger.error(f"Result enrichment failed: {e}", exc_info=True)
        return json.dumps({"error": f"Enrichment failed: {str(e)}"}, indent=2)


# ============================================================================
# Layer 5: Gateway Tools for Graph Navigation
# ============================================================================

def _register_gateway_tools():
    """Register gateway tools for graph navigation.

    Exposes 4 tools instead of 100+ individual functions:
    1. ground_entity - Natural language → CURIEs
    2. suggest_endpoints - Graph navigation suggestions
    3. call_endpoint - Execute any autoclient function
    4. get_navigation_schema - Full edge map for discovery
    """
    try:
        from .autoclient_tools import register_gateway_tools
        count = register_gateway_tools(mcp, get_client)
        logger.info(f"Registered {count} gateway tools for graph navigation")
    except Exception as e:
        logger.warning(f"Failed to register gateway tools: {e}")


# Register gateway tools when module is imported
_register_gateway_tools()


__all__ = ['mcp', 'get_client']
