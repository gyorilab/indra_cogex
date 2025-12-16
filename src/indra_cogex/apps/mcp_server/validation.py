"""Cypher query validation.

Minimal safety layer for Cypher queries. The Neo4j driver enforces read-only
semantics via execute_read(), so validation focuses on:
1. Catching dangerous keywords as defense-in-depth
2. Verifying parameterized query completeness

Neo4j provides superior error messages for syntax issues.
"""

import re
from typing import Any, Optional

__all__ = ["validate_cypher"]

# Operations that should never appear in read-only queries
DANGEROUS_KEYWORDS = frozenset({
    "DELETE", "DETACH", "CREATE", "MERGE", "SET", "REMOVE", "DROP",
})


def validate_cypher(
    query: str,
    parameters: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Validate Cypher query safety and parameter completeness.

    Parameters
    ----------
    query : str
        The Cypher query to validate
    parameters : Optional[dict[str, Any]]
        Query parameters for parameterized queries

    Returns
    -------
    dict[str, Any]
        Validation result: {"valid": bool, "issues": list[dict], "safe_to_execute": bool}
    """
    if not query or not query.strip():
        return {
            "valid": False,
            "issues": [{"severity": "error", "type": "empty_query", "message": "Query cannot be empty"}],
            "safe_to_execute": False,
        }

    issues = []
    query_upper = query.upper()

    # Check for dangerous operations (defense-in-depth)
    for keyword in DANGEROUS_KEYWORDS:
        if re.search(rf"\b{keyword}\b", query_upper):
            issues.append({
                "severity": "error",
                "type": "forbidden_operation",
                "message": f"{keyword} not allowed in read-only queries",
            })

    # Verify all referenced parameters are provided
    param_refs = set(re.findall(r"\$(\w+)", query))
    provided = set(parameters.keys()) if parameters else set()
    missing = param_refs - provided

    for param in missing:
        issues.append({
            "severity": "error",
            "type": "missing_parameter",
            "message": f"Parameter ${param} referenced but not provided",
        })

    has_errors = any(issue["severity"] == "error" for issue in issues)

    return {
        "valid": not has_errors,
        "issues": issues,
        "safe_to_execute": not has_errors,
    }
