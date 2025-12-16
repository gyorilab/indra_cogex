"""Result serialization for MCP gateway responses."""

import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def resolve_entity_names(
    results: List[Dict[str, Any]],
    client: Any,
) -> List[Dict[str, Any]]:
    """Batch-resolve entity names from Neo4j for results with db_ns/db_id.

    Queries the graph once for all entity IDs and merges names into results.
    Uses the existing name index on BioEntity nodes for fast lookups.

    Parameters
    ----------
    results : List[Dict[str, Any]]
        Processed results containing db_ns and db_id keys
    client : Neo4jClient
        Neo4j client for querying names

    Returns
    -------
    :
        Results with 'name' field populated where available
    """
    if not results or not client:
        return results

    # Collect all entity IDs that need name resolution
    ids_to_resolve = set()
    for item in results:
        if isinstance(item, dict) and "db_ns" in item and "db_id" in item:
            # Construct CURIE format used in graph: "namespace:id"
            entity_id = f"{item['db_ns'].lower()}:{item['db_id']}"
            if "name" not in item or item.get("name") is None:
                ids_to_resolve.add(entity_id)

    if not ids_to_resolve:
        return results

    # Batch query for names using core client method
    try:
        id_to_name = client.batch_get_entity_names(list(ids_to_resolve))

        logger.debug(f"Resolved {len(id_to_name)}/{len(ids_to_resolve)} entity names")

        # Merge names into results
        for item in results:
            if isinstance(item, dict) and "db_ns" in item and "db_id" in item:
                entity_id = f"{item['db_ns'].lower()}:{item['db_id']}"
                if entity_id in id_to_name:
                    item["name"] = id_to_name[entity_id]

    except Exception as e:
        logger.warning(f"Failed to resolve entity names: {e}")

    return results


def process_result(result: Any) -> Any:
    """Process query result to JSON-serializable format."""
    if result is None:
        return None
    if isinstance(result, bool):
        return result
    if isinstance(result, (str, int, float)):
        return result
    if isinstance(result, (list, tuple)):
        return [_process_item(item) for item in result]
    if isinstance(result, dict):
        return {k: _process_item(v) for k, v in result.items()}
    return _process_item(result)


def _process_item(item: Any) -> Any:
    """Process a single item to JSON-serializable format.

    Uses to_json() from representation.py (Node, Relation) and flattens
    Node's nested structure to MCP's flat format.
    """
    if item is None:
        return None
    if isinstance(item, bool):
        return item
    if isinstance(item, (str, int, float)):
        return item
    if isinstance(item, (list, tuple)):
        return [_process_item(i) for i in item]
    if isinstance(item, dict):
        return {k: _process_item(v) for k, v in item.items()}

    # Neo4j driver Node objects have callable .data() method
    if hasattr(item, 'data') and callable(item.data):
        return item.data()

    # INDRA representation classes (Node, Relation) have to_json()
    if hasattr(item, 'to_json'):
        json_output = item.to_json()
        # Flatten Node.to_json() nested structure: {"labels": [...], "data": {...}} -> {...}
        if isinstance(json_output, dict) and "labels" in json_output and "data" in json_output:
            return json_output["data"]
        return json_output

    # Named tuples
    if hasattr(item, '_asdict'):
        return _process_item(item._asdict())

    return str(item)


__all__ = ["process_result", "resolve_entity_names"]
