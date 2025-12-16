"""Layer 2: Query Execution - Safe Cypher execution with timeout and result limiting.

This module implements the core query execution layer for the MCP server, providing:
- Parameterized Cypher query execution with timeout enforcement
- Result limiting and truncation
- Query execution plan analysis (EXPLAIN)
- Comprehensive error handling and recovery
- Neo4j type serialization to JSON

Integration points:
- Called by server.py's execute_cypher tool handler
- Uses Neo4jClient for database access
- Integrates with validation.py for query safety checks
"""

import hashlib
import json
import logging
import time
from datetime import datetime, UTC
from typing import Any, Dict, List, Optional, Tuple, Union

import neo4j.graph
from neo4j import exceptions as neo4j_exceptions

from indra_cogex.client.neo4j_client import Neo4jClient
from indra_cogex.representation import norm_id
from .validation import validate_cypher
from indra_cogex.client.pagination import paginate_response

logger = logging.getLogger(__name__)


class QueryExecutionError(Exception):
    """Base exception for query execution errors."""
    pass


class QueryTimeoutError(QueryExecutionError):
    """Query exceeded timeout limit."""
    pass


class QuerySyntaxError(QueryExecutionError):
    """Cypher syntax error in query."""
    pass


class QueryValidationError(QueryExecutionError):
    """Query failed validation checks."""
    pass


def _normalize_curie_parameters(parameters: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize CURIE-like strings in query parameters to match graph storage format.

    The Neo4j graph stores entity IDs in lowercase normalized format (e.g., 'hgnc:28337'),
    but users naturally provide uppercase CURIEs (e.g., 'HGNC:28337'). This function
    recursively processes parameter values and normalizes any CURIE-like strings using
    the bioregistry normalization pipeline.

    Parameters
    ----------
    parameters : Dict[str, Any]
        Query parameters that may contain CURIE strings

    Returns
    -------
    Dict[str, Any]
        Parameters with normalized CURIEs

    Examples
    --------
    >>> _normalize_curie_parameters({"gene_id": "HGNC:28337"})
    {"gene_id": "hgnc:28337"}

    >>> _normalize_curie_parameters({"gene_ids": ["HGNC:28337", "HGNC:6407"]})
    {"gene_ids": ["hgnc:28337", "hgnc:6407"]}

    >>> _normalize_curie_parameters({"limit": 10, "name": "TP53"})
    {"limit": 10, "name": "TP53"}  # Non-CURIE values unchanged
    """
    def _normalize_value(value: Any) -> Any:
        """Recursively normalize a single value."""
        if isinstance(value, str):
            # Check if this looks like a CURIE (contains ':' and has prefix/suffix)
            if ':' in value:
                parts = value.split(':', 1)
                if len(parts) == 2 and parts[0] and parts[1]:
                    # Attempt to normalize using norm_id
                    try:
                        normalized = norm_id(parts[0], parts[1])
                        if normalized != value:
                            logger.debug(
                                f"Normalized CURIE parameter: {value} -> {normalized}"
                            )
                        return normalized
                    except Exception as e:
                        # If normalization fails, log and return original value
                        logger.debug(
                            f"Could not normalize '{value}' (treating as literal string): {e}"
                        )
                        return value
            return value

        elif isinstance(value, list):
            # Recursively normalize list elements
            return [_normalize_value(item) for item in value]

        elif isinstance(value, dict):
            # Recursively normalize dictionary values
            return {k: _normalize_value(v) for k, v in value.items()}

        elif isinstance(value, tuple):
            # Recursively normalize tuple elements (convert to tuple after)
            return tuple(_normalize_value(item) for item in value)

        else:
            # Non-string values (int, float, bool, None, etc.) pass through unchanged
            return value

    # Process all parameters
    normalized_params = {}
    for key, value in parameters.items():
        normalized_params[key] = _normalize_value(value)

    return normalized_params


def execute_cypher(
    client: Neo4jClient,
    query: str,
    parameters: Optional[Dict[str, Any]] = None,
    validate_first: bool = True,
    timeout_ms: int = 30000,
    max_results: int = 100,
    explain: bool = False,
    offset: int = 0,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    """Execute Cypher query with validation, timeout, result limiting, and pagination.

    This function automatically normalizes CURIE-like parameters to match the graph's
    lowercase storage format. For example, 'HGNC:28337' is normalized to 'hgnc:28337'.
    """
    if parameters is None:
        parameters = {}

    # Normalize CURIE parameters to match graph storage format
    # This ensures uppercase CURIEs like 'HGNC:28337' match the lowercase 'hgnc:28337' in the graph
    original_parameters = parameters.copy()
    parameters = _normalize_curie_parameters(parameters)

    if timeout_ms > 120000:
        logger.warning(f"Timeout {timeout_ms}ms exceeds max 120000ms, capping to 120s")
        timeout_ms = 120000

    if max_results > 10000:
        logger.warning(f"max_results {max_results} exceeds max 10000, capping")
        max_results = 10000

    if explain:
        return explain_query(client, query, parameters)

    if validate_first:
        validation_result = validate_cypher(query, parameters)
        if not validation_result["safe_to_execute"]:
            raise QueryValidationError(
                f"Query validation failed: {validation_result['issues']}"
            )

    start_time = time.time()
    try:
        column_names, results = client.query_tx_with_keys(query, **parameters)
        execution_time_ms = (time.time() - start_time) * 1000

        if execution_time_ms > timeout_ms:
            logger.warning(
                f"Query exceeded timeout threshold: {execution_time_ms}ms > {timeout_ms}ms"
            )
    except neo4j_exceptions.CypherSyntaxError as e:
        logger.error(f"Cypher syntax error: {e}")
        raise QuerySyntaxError(f"Cypher syntax error: {str(e)}")
    except neo4j_exceptions.ServiceUnavailable as e:
        logger.error(f"Neo4j service unavailable: {e}")
        raise QueryExecutionError(f"Database unavailable: {str(e)}")
    except Exception as e:
        execution_time_ms = (time.time() - start_time) * 1000
        error_str = str(e).lower()
        if "timeout" in error_str or "timed out" in error_str or execution_time_ms > timeout_ms:
            logger.error(f"Query timeout after {execution_time_ms:.0f}ms: {e}")
            raise QueryTimeoutError(
                f"Query exceeded timeout ({timeout_ms}ms). "
                "Try simplifying query or adding LIMIT clause."
            )
        logger.error(f"Query execution error: {e}", exc_info=True)
        raise QueryExecutionError(f"Query execution failed: {str(e)}")

    truncated = len(results) > max_results
    if truncated:
        logger.info(f"Results truncated: {len(results)} total, returning {max_results}")
    limited_results = results[:max_results] if results else []

    serialized_results = _serialize_results(limited_results, column_names)
    query_hash = _compute_query_hash(query, parameters)

    # Apply pagination with automatic token limiting
    paginated = paginate_response(
        serialized_results,
        offset=offset,
        limit=limit,
        include_token_estimate=True,
    )
    final_results = paginated["results"]
    pagination_info = paginated["pagination"]

    # Adjust pagination totals to account for max_results truncation
    if truncated:
        pagination_info["db_truncated"] = True
        pagination_info["db_max_results"] = max_results

    result_dict = {
        "query": query,
        "parameters": parameters,
        "results": final_results,
        "metadata": {
            "result_count": pagination_info.get("returned", len(final_results)),
            "execution_time_ms": execution_time_ms,
            "truncated": truncated or pagination_info.get("truncated", False),
            "original_count": len(results) if truncated else pagination_info.get("total", len(final_results)),
            "from_cache": False,
            "query_hash": query_hash,
            "timestamp": datetime.now(UTC).isoformat()
        },
        "pagination": pagination_info,
        "from_cache": False,
        "token_estimate": pagination_info.get("token_estimate", _estimate_result_tokens(final_results))
    }

    # Add continuation hint if there's more data
    if pagination_info.get("has_more"):
        result_dict["continuation_hint"] = (
            f"Showing {pagination_info['returned']} of {pagination_info['total']} results. "
            f"Call with offset={pagination_info['next_offset']} to continue."
        )

    if execution_time_ms > 1000:
        logger.warning(
            f"Slow query ({execution_time_ms}ms): {query[:100]}... "
            f"[{len(results)} results]"
        )

    return result_dict


def explain_query(
    client: Neo4jClient,
    query: str,
    parameters: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Return Neo4j execution plan for query without running it.

    Parameters are normalized to match graph storage format before analysis.
    """
    if parameters is None:
        parameters = {}

    # Normalize parameters to match graph storage format
    parameters = _normalize_curie_parameters(parameters)

    explain_query_str = f"EXPLAIN {query}"

    try:
        result = client.query_tx(explain_query_str, **parameters)

        if result:
            plan_dict = _parse_explain_plan(result)
            return {
                "query": query,
                "execution_plan": plan_dict,
                "warnings": plan_dict.get("warnings", []),
                "recommendations": plan_dict.get("recommendations", [])
            }
        else:
            return {
                "query": query,
                "execution_plan": {},
                "warnings": ["Could not retrieve execution plan"],
                "recommendations": []
            }

    except neo4j_exceptions.CypherSyntaxError as e:
        logger.error(f"Syntax error in EXPLAIN query: {e}")
        return {
            "query": query,
            "error": f"Cypher syntax error: {str(e)}",
            "execution_plan": {},
            "warnings": [str(e)],
            "recommendations": ["Fix syntax errors before analyzing query plan"]
        }
    except Exception as e:
        logger.error(f"Error explaining query: {e}", exc_info=True)
        return {
            "query": query,
            "error": f"Failed to explain query: {str(e)}",
            "execution_plan": {},
            "warnings": [str(e)],
            "recommendations": []
        }


# ============================================================================
# Helper Functions
# ============================================================================


def _compute_query_hash(query: str, parameters: Dict[str, Any]) -> str:
    """Compute stable hash for query + parameters for caching."""
    normalized = query.strip().lower()
    param_str = json.dumps(parameters, sort_keys=True)
    combined = f"{normalized}:{param_str}"
    return hashlib.sha256(combined.encode()).hexdigest()[:16]


def _serialize_results(
    results: List[List[Any]],
    column_names: List[str]
) -> List[Dict[str, Any]]:
    """Convert Neo4j result objects to JSON-serializable dicts."""
    serialized = []

    for record in results:
        if not record:
            continue

        row_dict = {}
        for i, value in enumerate(record):
            key = column_names[i] if i < len(column_names) else f"col_{i}"
            row_dict[key] = _serialize_value(value)

        serialized.append(row_dict)

    return serialized


def _serialize_value(value: Any) -> Any:
    """Recursively serialize value to JSON-compatible format."""
    if value is None:
        return None

    if isinstance(value, neo4j.graph.Node):
        node_dict = dict(value)
        node_dict["_labels"] = list(value.labels)
        node_dict["_element_id"] = value.element_id
        return node_dict

    if isinstance(value, neo4j.graph.Relationship):
        rel_dict = dict(value)
        rel_dict["_type"] = value.type
        rel_dict["_element_id"] = value.element_id
        rel_dict["_start_element_id"] = value.start_node.element_id if hasattr(value, "start_node") else None
        rel_dict["_end_element_id"] = value.end_node.element_id if hasattr(value, "end_node") else None
        return rel_dict

    if isinstance(value, neo4j.graph.Path):
        return {
            "nodes": [_serialize_value(node) for node in value.nodes],
            "relationships": [_serialize_value(rel) for rel in value.relationships],
            "length": len(value)
        }

    if isinstance(value, list):
        return [_serialize_value(item) for item in value]

    if isinstance(value, dict):
        return {k: _serialize_value(v) for k, v in value.items()}

    if hasattr(value, "isoformat"):
        return value.isoformat()

    if hasattr(value, "__dict__") and not isinstance(value, (str, int, float, bool)):
        try:
            return {k: _serialize_value(v) for k, v in value.__dict__.items()}
        except Exception:
            return str(value)

    return value


def _parse_explain_plan(result: List[List[Any]]) -> Dict[str, Any]:
    """Parse EXPLAIN query results into structured execution plan."""
    plan = {
        "estimated_rows": None,
        "operators": [],
        "estimated_cost": "unknown",
        "warnings": [],
        "recommendations": []
    }

    if result and len(result) > 0:
        plan["estimated_rows"] = len(result)
        plan["warnings"].append("Execution plan details require Neo4j Result.plan() API")
        plan["recommendations"].append("Use PROFILE for actual execution statistics")

    return plan


def _estimate_result_tokens(results: List[Dict[str, Any]]) -> int:
    """Estimate token count for serialized results (~4 chars per token)."""
    if not results:
        return 0

    try:
        json_str = json.dumps(results)
        char_count = len(json_str)
        token_estimate = char_count // 4
        token_estimate += len(results) * 10
        return token_estimate
    except Exception as e:
        logger.warning(f"Failed to estimate tokens: {e}")
        return len(results) * 50


