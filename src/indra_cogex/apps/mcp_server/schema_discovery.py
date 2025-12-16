"""Layer 1: Schema Discovery - Progressive graph schema introspection."""

import json
import logging
import time
from typing import Any, Dict, List, Optional

from indra_cogex.client.neo4j_client import Neo4jClient
from indra_cogex.client.queries import get_node_counter, get_edge_counter, get_schema_graph

logger = logging.getLogger(__name__)


def get_graph_schema(
    client: Neo4jClient,
    detail_level: str = "summary",
    entity_type: Optional[str] = None,
    relationship_type: Optional[str] = None,
) -> Dict[str, Any]:
    """Get schema at specified detail level (summary/entity_types/relationship_types/patterns/full)."""
    start_time = time.time()

    if detail_level == "summary":
        result = _get_summary_schema(client)
    elif detail_level == "entity_types":
        result = _get_entity_types_schema(client, entity_type)
    elif detail_level == "relationship_types":
        result = _get_relationship_types_schema(client, relationship_type)
    elif detail_level == "patterns":
        result = _get_patterns_schema(client, relationship_type)
    elif detail_level == "full":
        result = _get_full_schema(client, entity_type, relationship_type)
    else:
        raise ValueError(
            f"Invalid detail_level: {detail_level}. "
            "Must be one of: summary, entity_types, relationship_types, patterns, full"
        )

    result["detail_level"] = detail_level
    result["execution_time_ms"] = int((time.time() - start_time) * 1000)
    result["token_estimate"] = _estimate_tokens(result)

    return result


def _get_summary_schema(client: Neo4jClient) -> Dict[str, Any]:
    """Get summary-level schema (entity/relationship type names only)."""
    try:
        node_counter = dict(get_node_counter(client=client))
    except Exception as e:
        logger.warning(f"Failed to get node counter: {e}")
        node_counter = {}

    try:
        edge_counter = dict(get_edge_counter(client=client))
    except Exception as e:
        logger.warning(f"Failed to get edge counter: {e}")
        edge_counter = {}

    labels = sorted(node_counter.keys())
    rel_types = sorted(edge_counter.keys())

    total_entities = sum(node_counter.values()) if node_counter else None
    total_relationships = sum(edge_counter.values()) if edge_counter else None

    return {
        "entity_types": labels,
        "relationship_types": rel_types,
        "total_entities": total_entities,
        "total_relationships": total_relationships,
    }


def _get_entity_types_schema(
    client: Neo4jClient, entity_type: Optional[str] = None
) -> Dict[str, Any]:
    """Get entity types with properties and counts."""
    result = _get_summary_schema(client)

    entity_details = []

    if entity_type:
        labels = [entity_type]
        include_counts = True
    else:
        labels = result["entity_types"][:30]
        include_counts = False

    for label in labels:
        try:
            count = None
            if include_counts:
                try:
                    node_counter = dict(get_node_counter(client=client))
                    count = node_counter.get(label, 0)
                except Exception as e:
                    logger.warning(f"Failed to get node count for {label}: {e}")
                    count = 0

            properties = _get_node_properties(client, label)
            sample_ids = _sample_entity_ids(client, label, limit=5)

            entity_details.append({
                "label": label,
                "count": count,
                "properties": properties,
                "sample_ids": sample_ids,
            })
        except Exception as e:
            logger.warning(f"Failed to get details for entity type {label}: {e}")

    result["entity_details"] = entity_details
    return result


def _get_relationship_types_schema(
    client: Neo4jClient, relationship_type: Optional[str] = None
) -> Dict[str, Any]:
    """Get relationship types with properties and endpoints using db.schema.visualization()."""
    result = _get_summary_schema(client)

    # Get schema graph for fast endpoint lookup
    try:
        schema_graph = get_schema_graph(client=client)
    except Exception as e:
        logger.warning(f"Failed to get schema graph: {e}")
        schema_graph = None

    relationship_details = []

    if relationship_type:
        rel_types = [relationship_type]
        include_counts = True
    else:
        rel_types = result["relationship_types"][:20]
        include_counts = False

    for rel_type in rel_types:
        try:
            count = None
            if include_counts:
                try:
                    edge_counter = dict(get_edge_counter(client=client))
                    count = edge_counter.get(rel_type, 0)
                except Exception:
                    pass

            properties = _get_relationship_properties(client, rel_type)

            # Get endpoints from schema graph (fast!)
            source_types, target_types = _get_relationship_endpoints_from_schema(
                schema_graph, rel_type
            )

            relationship_details.append({
                "type": rel_type,
                "count": count,
                "properties": properties,
                "source_types": source_types,
                "target_types": target_types,
            })
        except Exception as e:
            logger.warning(f"Failed to get details for relationship type {rel_type}: {e}")

    result["relationship_details"] = relationship_details
    return result


def _get_patterns_schema(
    client: Neo4jClient,
    relationship_type: Optional[str] = None,
) -> Dict[str, Any]:
    """Get relationship patterns using db.schema.visualization()."""
    result = _get_summary_schema(client)

    # Get schema graph for fast pattern extraction
    try:
        schema_graph = get_schema_graph(client=client)
    except Exception as e:
        logger.warning(f"Failed to get schema graph: {e}")
        result["relationship_patterns"] = []
        return result

    relationship_patterns = []

    if relationship_type:
        rel_types = [relationship_type]
    else:
        rel_types = result["relationship_types"][:20]

    for rel_type in rel_types:
        try:
            patterns = _extract_patterns_from_schema_graph(schema_graph, rel_type)
            relationship_patterns.extend(patterns)
        except Exception as e:
            logger.warning(f"Failed to get patterns for relationship type {rel_type}: {e}")

    result["relationship_patterns"] = relationship_patterns
    return result


def _get_full_schema(
    client: Neo4jClient,
    entity_type: Optional[str] = None,
    relationship_type: Optional[str] = None,
) -> Dict[str, Any]:
    """Get full schema with all details."""
    result = _get_entity_types_schema(client, entity_type)

    rel_result = _get_relationship_types_schema(client, relationship_type)
    result["relationship_details"] = rel_result["relationship_details"]

    patterns_result = _get_patterns_schema(client, relationship_type)
    result["relationship_patterns"] = patterns_result.get("relationship_patterns", [])

    return result


def _get_node_properties(client: Neo4jClient, label: str) -> List[str]:
    """Get property names for a node label."""
    try:
        query = f"MATCH (n:{label}) RETURN keys(n) LIMIT 1"
        result = client.query_tx(query, squeeze=True)
        if result and result[0]:
            return sorted(result[0])
    except Exception as e:
        logger.warning(f"Failed to get node properties for {label}: {e}")

    return []


def _get_relationship_properties(client: Neo4jClient, rel_type: str) -> List[str]:
    """Get property names for a relationship type."""
    try:
        query = f"MATCH ()-[r:{rel_type}]->() RETURN keys(r) LIMIT 1"
        result = client.query_tx(query, squeeze=True)
        if result and result[0]:
            return sorted(result[0])
    except Exception as e:
        logger.warning(f"Failed to get relationship properties for {rel_type}: {e}")

    return []


def _get_relationship_endpoints_from_schema(
    schema_graph, rel_type: str
) -> tuple[List[str], List[str]]:
    """Extract relationship endpoints from schema graph (fast!)."""
    if schema_graph is None:
        return [], []

    source_types = set()
    target_types = set()

    try:
        # Iterate through edges in the schema graph
        for source, target, edge_data in schema_graph.edges(data=True):
            if edge_data.get("label") == rel_type:
                # Get the node labels
                source_label = schema_graph.nodes[source].get("label")
                target_label = schema_graph.nodes[target].get("label")

                if source_label:
                    source_types.add(source_label)
                if target_label:
                    target_types.add(target_label)

        return sorted(source_types), sorted(target_types)
    except Exception as e:
        logger.warning(f"Failed to extract endpoints from schema graph for {rel_type}: {e}")
        return [], []


def _extract_patterns_from_schema_graph(
    schema_graph, rel_type: str
) -> List[Dict[str, Any]]:
    """Extract relationship patterns from schema graph (fast!)."""
    if schema_graph is None:
        return []

    patterns = []

    try:
        # Find all edges with this relationship type
        for source, target, edge_data in schema_graph.edges(data=True):
            if edge_data.get("label") == rel_type:
                source_label = schema_graph.nodes[source].get("label", "Node")
                target_label = schema_graph.nodes[target].get("label", "Node")

                pattern_str = f"({source_label})-[:{rel_type}]->({target_label})"

                example_query, description = _generate_example_query(
                    source_label, rel_type, target_label
                )

                pattern_dict = {
                    "pattern": pattern_str,
                    "count": None,  # Schema doesn't have counts
                    "description": description,
                    "example_query": example_query,
                }

                patterns.append(pattern_dict)

    except Exception as e:
        logger.warning(f"Failed to extract patterns from schema graph for {rel_type}: {e}")

    return patterns


def _sample_entity_ids(client: Neo4jClient, label: str, limit: int = 5) -> List[str]:
    """Get sample entity IDs for a label."""
    try:
        query = f"MATCH (n:{label}) WHERE n.id IS NOT NULL RETURN n.id LIMIT {limit}"
        result = client.query_tx(query, squeeze=True)
        return result
    except Exception as e:
        logger.warning(f"Failed to sample entity IDs for label {label}: {e}")
        return []


def _estimate_tokens(schema_dict: Dict[str, Any]) -> int:
    """Estimate JSON token count (4 chars per token heuristic)."""
    try:
        json_str = json.dumps(schema_dict, indent=None)
        return len(json_str) // 4
    except Exception as e:
        logger.warning(f"Failed to estimate tokens: {e}")
        return len(str(schema_dict)) // 4


def _generate_example_query(
    source_label: str,
    rel_type: str,
    target_label: str,
) -> tuple[str, str]:
    """Generate an example Cypher query for a relationship pattern."""
    description = f"{source_label} connected to {target_label} via {rel_type}"

    if rel_type == "indra_rel" and "BioEntity" in source_label and "BioEntity" in target_label:
        query = f"""MATCH (a:{source_label})-[r:{rel_type}]->(b:{target_label})
WHERE r.stmt_type IS NOT NULL
RETURN a.id AS source_id, a.name AS source_name,
       r.stmt_type AS interaction_type,
       r.evidence_count AS evidence_count,
       b.id AS target_id, b.name AS target_name
ORDER BY r.evidence_count DESC
LIMIT 10"""
        description = f"Protein-protein interactions and other biological relationships from INDRA"

    elif rel_type == "gene_disease_association":
        query = f"""MATCH (g:{source_label})-[r:{rel_type}]->(d:{target_label})
WHERE g.id STARTS WITH 'hgnc:' AND d.id STARTS WITH 'mesh:'
RETURN g.id AS gene_id, g.name AS gene_name,
       d.id AS disease_id, d.name AS disease_name
LIMIT 10"""
        description = "Genes associated with diseases"

    elif "pathway" in rel_type.lower():
        query = f"""MATCH (g:{source_label})-[r:{rel_type}]->(p:{target_label})
RETURN g.id AS gene_id, g.name AS gene_name,
       p.id AS pathway_id, p.name AS pathway_name
LIMIT 10"""
        description = f"Genes in pathways"

    elif "drug" in source_label.lower() or "drug" in target_label.lower():
        query = f"""MATCH (d:{source_label})-[r:{rel_type}]->(t:{target_label})
RETURN d.id AS drug_id, d.name AS drug_name,
       t.id AS target_id, t.name AS target_name
LIMIT 10"""
        description = "Drug-target relationships"

    else:
        query = f"""MATCH (a:{source_label})-[r:{rel_type}]->(b:{target_label})
RETURN a.id AS source_id, a.name AS source_name,
       b.id AS target_id, b.name AS target_name
LIMIT 10"""

    return query, description
