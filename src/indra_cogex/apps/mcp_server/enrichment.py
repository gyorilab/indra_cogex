"""Result enrichment layer for biomedical query results.

Progressive metadata enrichment for query results at four disclosure levels
to optimize token efficiency: MINIMAL (~4k), STANDARD (~8k), DETAILED (~15k), EXPLORATORY (~30k).
"""
from enum import Enum
from typing import Any, Dict, List, Optional
import logging

from indra_cogex.client.pagination import paginate_response, estimate_tokens

logger = logging.getLogger(__name__)

# Entity type constants
NAMESPACE_TO_TYPE = {"hgnc": "gene", "ncbigene": "gene", "mesh": "disease", "doid": "disease", "mondo": "disease", "drugbank": "drug", "chebi": "drug", "pubchem.compound": "drug", "go": "go_term", "reactome": "pathway", "wikipathways": "pathway"}
ENTITY_DESCRIPTIONS = {"gene": "Gene entity from biomedical knowledge graph", "disease": "Disease entity from biomedical knowledge graph", "drug": "Drug/chemical entity from biomedical knowledge graph", "pathway": "Biological pathway entity from biomedical knowledge graph", "go_term": "Gene Ontology term"}

# Metadata mappings
NEXT_STEPS_STANDARD = {"gene": ["Find diseases: Query gene-disease associations", "Find pathways: Query gene-pathway memberships"], "disease": ["Find genes: Query disease-gene associations", "Find drugs: Query disease treatments"], "drug": ["Find targets: Query drug-target interactions", "Find indications: Query drug-disease indications"], "pathway": ["Find genes: Query pathway member genes", "Find diseases: Query pathway-disease associations"]}
NEXT_STEPS_DETAILED = {"gene": ["Find diseases: Query gene-disease associations", "Find pathways: Query gene-pathway memberships", "Find drugs: Query drugs targeting this gene", "Find GO terms: Query gene ontology annotations"], "disease": ["Find genes: Query disease-gene associations", "Find drugs: Query disease treatments", "Find phenotypes: Query disease phenotypes", "Find trials: Query clinical trials"], "drug": ["Find targets: Query drug-target interactions", "Find indications: Query drug-disease indications", "Find side effects: Query adverse drug reactions", "Find trials: Query clinical trials"], "pathway": ["Find genes: Query pathway member genes", "Find diseases: Query pathway-disease associations", "Find drugs: Query pathway-targeting drugs"]}
WORKFLOWS = {"gene": ["Disease research: gene → pathways → diseases (find disease modules)", "Drug discovery: gene → drugs → side effects (assess therapeutic options)"], "disease": ["Mechanism: disease → genes → pathways (understand biology)", "Treatment: disease → drugs → targets (find interventions)"], "drug": ["Safety: drug → side effects (assess risks)", "Mechanism: drug → targets → pathways (understand action)"], "pathway": ["Analysis: pathway → genes → diseases (find disease pathways)"]}
COMMON_QUERIES = {"gene": ["Find diseases associated with gene", "Find pathways containing gene", "Find drugs targeting gene"], "disease": ["Find genes associated with disease", "Find drugs treating disease", "Find disease phenotypes"], "drug": ["Find drug targets", "Find drug indications", "Find drug side effects"], "pathway": ["Find pathway genes", "Find diseases involving pathway"]}
RELATED_FIELDS = {"gene": ["genomics", "proteomics", "systems biology"], "disease": ["clinical research", "epidemiology", "precision medicine"], "drug": ["pharmacology", "drug discovery", "toxicology"], "pathway": ["systems biology", "molecular biology", "biochemistry"]}

__all__ = ["DisclosureLevel", "enrich_results"]


class DisclosureLevel(str, Enum):
    """Progressive metadata disclosure levels."""
    MINIMAL = "minimal"
    STANDARD = "standard"
    DETAILED = "detailed"
    EXPLORATORY = "exploratory"


def enrich_results(
    results: list,
    disclosure_level: DisclosureLevel = DisclosureLevel.STANDARD,
    result_type: Optional[str] = None,
    client: Any = None,
    offset: int = 0,
    limit: Optional[int] = None,
) -> dict:
    """Add progressive metadata to query results with pagination support.

    Parameters
    ----------
    results : list
        Query results to enrich
    disclosure_level : DisclosureLevel
        Metadata verbosity level
    result_type : str, optional
        Entity type (auto-inferred if not provided)
    client : Any, optional
        Neo4j client for additional lookups
    offset : int
        Starting offset for pagination (default: 0)
    limit : int, optional
        Maximum items per page. Auto-truncates to ~20k tokens if exceeded.

    Returns
    -------
    :
        Dict with results, disclosure_level, metadata, pagination, and token_estimate
    """
    # Handle string disclosure level for backwards compatibility
    if isinstance(disclosure_level, str):
        try:
            disclosure_level = DisclosureLevel(disclosure_level)
        except ValueError:
            raise ValueError(f"Invalid disclosure_level: {disclosure_level}. Must be one of: {[e.value for e in DisclosureLevel]}")

    if result_type is None:
        result_type = _infer_result_type(results)

    enriched = []
    for item in results:
        if disclosure_level == DisclosureLevel.MINIMAL:
            enriched.append(item)
        elif disclosure_level == DisclosureLevel.STANDARD:
            enriched_item = _add_standard_metadata(item, result_type, client)
            enriched.append(enriched_item)
        elif disclosure_level == DisclosureLevel.DETAILED:
            enriched_item = _add_detailed_metadata(item, result_type, client)
            enriched.append(enriched_item)
        elif disclosure_level == DisclosureLevel.EXPLORATORY:
            enriched_item = _add_exploratory_metadata(item, result_type, client)
            enriched.append(enriched_item)

    # Apply pagination with automatic token limiting
    paginated = paginate_response(
        enriched,
        offset=offset,
        limit=limit,
        include_token_estimate=True,
    )

    final_results = paginated["results"]
    pagination_info = paginated["pagination"]

    metadata = {"result_count": pagination_info.get("returned", len(final_results)), "result_type": result_type or "unknown"}
    if result_type and result_type in ENTITY_DESCRIPTIONS:
        metadata["result_type_hint"] = ENTITY_DESCRIPTIONS[result_type]

    response = {
        "results": final_results,
        "disclosure_level": disclosure_level.value,
        "metadata": metadata,
        "pagination": pagination_info,
        "token_estimate": pagination_info.get("token_estimate", estimate_tokens(final_results))
    }

    # Add continuation hint if there's more data
    if pagination_info.get("has_more"):
        response["continuation_hint"] = (
            f"Showing {pagination_info['returned']} of {pagination_info['total']} results. "
            f"Call with offset={pagination_info['next_offset']} to continue."
        )

    return response


def _infer_result_type(results: list) -> Optional[str]:
    """Infer entity type from results.

    Handles multiple result formats:
    - {"id": "hgnc:1234"} - CURIE string
    - {"id": ["HGNC", "1234"]} - CURIE tuple
    - {"db_ns": "HGNC", "db_id": "1234"} - INDRA style (from autoclient)
    """
    if not results:
        return None

    first = results[0]
    if not isinstance(first, dict):
        return None

    
    # Check for INDRA-style db_ns/db_id format (from autoclient results)
    if "db_ns" in first:
        namespace = first["db_ns"].lower()
        if namespace in NAMESPACE_TO_TYPE:
            return NAMESPACE_TO_TYPE[namespace]

    # Check for CURIE-style id or *_id fields
    for key, value in first.items():
        if key == "id" or key.endswith("_id"):
            entity_id = value
            if isinstance(entity_id, str) and ":" in entity_id:
                namespace = entity_id.split(":", 1)[0].lower()
                if namespace in NAMESPACE_TO_TYPE:
                    return NAMESPACE_TO_TYPE[namespace]
            elif isinstance(entity_id, (tuple, list)) and len(entity_id) == 2:
                namespace = entity_id[0].lower()
                if namespace in NAMESPACE_TO_TYPE:
                    return NAMESPACE_TO_TYPE[namespace]

    return None


def _add_standard_metadata(item: dict, result_type: str, client: Any) -> dict:
    """Add standard-level metadata to a single result item."""
    if not isinstance(item, dict):
        return item

    enriched = item.copy()
    if "_description" not in enriched and result_type:
        enriched["_description"] = ENTITY_DESCRIPTIONS.get(result_type, f"{result_type.capitalize()} entity from CoGEx knowledge graph")
    if "_next_steps" not in enriched:
        enriched["_next_steps"] = _generate_next_steps(item, result_type, level="standard")
    return enriched


def _add_detailed_metadata(item: dict, result_type: str, client: Any) -> dict:
    """Add detailed-level metadata with provenance.

    Note: _graph_context was removed as it provided minimal value (only echoed
    entity_type/entity_id) while the client param was unused. For actual graph
    topology analysis, use dedicated analysis tools or execute_cypher directly.
    """
    enriched = _add_standard_metadata(item, result_type, client)
    if not isinstance(enriched, dict):
        return enriched

    if "_provenance" not in enriched:
        enriched["_provenance"] = {"source": "CoGEx Knowledge Graph", "entity_type": result_type or "unknown"}
    enriched["_next_steps"] = _generate_next_steps(item, result_type, level="detailed")
    return enriched


def _add_exploratory_metadata(item: dict, result_type: str, client: Any) -> dict:
    """Add exploratory-level metadata with workflows for autonomous graph exploration."""
    enriched = _add_detailed_metadata(item, result_type, client)
    if not isinstance(enriched, dict):
        return enriched

    if "_workflows" not in enriched:
        enriched["_workflows"] = WORKFLOWS.get(result_type, [])
    if "_research_context" not in enriched:
        enriched["_research_context"] = {"common_queries": COMMON_QUERIES.get(result_type, ["Explore entity relationships"]), "related_fields": RELATED_FIELDS.get(result_type, ["biomedical research"])}
    return enriched


def _generate_next_steps(item: dict, result_type: str, level: str = "standard") -> List[str]:
    """Generate suggested next queries based on entity type."""
    if not result_type:
        return ["Explore relationships: Use graph traversal to find connections"]
    steps_map = NEXT_STEPS_DETAILED if level == "detailed" else NEXT_STEPS_STANDARD
    return steps_map.get(result_type, [f"Explore {result_type} relationships in graph"])
