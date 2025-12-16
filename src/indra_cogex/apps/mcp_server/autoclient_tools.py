"""Single gateway for autoclient endpoints with graph navigation.

Progressive disclosure pattern for agent interaction:
1. ground_entity: Natural language → CURIEs (GILDA)
2. suggest_endpoints: "You have Gene entities. You can explore: Disease, Pathway..."
3. call_endpoint: Execute function with auto-grounding

Graph navigation approach:
- Functions like get_diseases_for_gene define edges: Gene → Disease
- Agents navigate the knowledge graph by following these edges
- suggest_endpoints shows WHERE you can go, not just WHAT you can call
"""

import asyncio
import json
import logging
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from .mappings import (
    CURIE_PREFIX_TO_ENTITY,
    PARAM_NAMESPACE_FILTERS,
    MIN_CONFIDENCE_THRESHOLD,
    AMBIGUITY_SCORE_THRESHOLD,
)
from .registry import _get_registry, clear_registry_cache
from .serialization import process_result, resolve_entity_names
from indra_cogex.client.pagination import paginate_response, estimate_tokens

logger = logging.getLogger(__name__)


def _detect_entity_types(entity_ids: List[str]) -> Set[str]:
    """Detect entity types from CURIE prefixes."""
    detected = set()
    for eid in entity_ids:
        eid_lower = eid.lower()
        for prefix, entity_type in CURIE_PREFIX_TO_ENTITY.items():
            if eid_lower.startswith(prefix + ':') or eid_lower.startswith(prefix + '_'):
                detected.add(entity_type)
                break
    return detected


def _lookup_xrefs(
    namespace: str,
    identifier: str,
    client,
    param_name: Optional[str] = None,
) -> List[Tuple[str, str]]:
    """Look up equivalent identifiers via xref relationships in the graph.

    Parameters
    ----------
    namespace : str
        Source namespace (e.g., "mesh", "doid")
    identifier : str
        Source identifier
    client
        Neo4j client
    param_name : str, optional
        Parameter name for filtering (e.g., "disease" filters to disease namespaces)

    Returns
    -------
    :
        List of (namespace, id) tuples including the original plus any xrefs found
    """
    from indra_cogex.representation import norm_id

    # Start with the original identifier
    equivalents = [(namespace.lower(), identifier)]

    # Get allowed namespaces for this parameter type
    allowed_namespaces = None
    if param_name:
        allowed_namespaces = PARAM_NAMESPACE_FILTERS.get(param_name.lower())

    try:
        # Query for xref relationships (bidirectional)
        source_id = norm_id(namespace, identifier)
        query = """
            MATCH (source:BioEntity {id: $source_id})-[:xref]-(target:BioEntity)
            RETURN target.id AS target_id
            LIMIT 20
        """

        results = client.query_tx(query, source_id=source_id, squeeze=True)

        for target_id in results:
            if ":" in target_id:
                target_ns, target_identifier = target_id.split(":", 1)
                target_ns_lower = target_ns.lower()

                # Filter by allowed namespaces if specified
                if allowed_namespaces and target_ns_lower not in allowed_namespaces:
                    continue

                equivalents.append((target_ns_lower, target_identifier))

        logger.debug(f"Found {len(equivalents)-1} xrefs for {namespace}:{identifier}")

    except Exception as e:
        logger.warning(f"xref lookup failed for {namespace}:{identifier}: {e}")

    return equivalents


def suggest_endpoints(
    entity_ids: List[str],
    intent: Optional[str] = None,
    top_k: int = 10,
) -> Dict[str, Any]:
    """Suggest navigation options from current entities.

    This is GRAPH NAVIGATION, not search. Given entities you have,
    shows where you can go next in the knowledge graph.

    Parameters
    ----------
    entity_ids : List[str]
        CURIEs from previous results (e.g., ["HGNC:6407", "MESH:D010300"])
    intent : str, optional
        Exploration goal (e.g., "find drug targets", "understand mechanisms")
    top_k : int
        Max suggestions per source entity type

    Returns
    -------
    :
        Dict with source_entities, navigation_options, total_sources,
        intent, and hint fields
    """
    registry, func_mapping, edge_map = _get_registry()

    if not registry:
        return {"error": "Function registry not available", "navigation_options": []}

    # Detect what entity types we have
    source_types = _detect_entity_types(entity_ids)

    if not source_types:
        return {
            "error": "Could not detect entity types from IDs. Use CURIE format (e.g., HGNC:6407)",
            "hint": "Ground natural language terms first using ground_entity",
            "entity_ids_received": entity_ids[:5],
        }

    # Build navigation options grouped by source → target
    navigation = []
    for source_type in sorted(source_types):
        if source_type not in edge_map:
            continue

        source_nav = {
            "from": source_type,
            "can_reach": [],
        }

        for target_type, functions in sorted(edge_map[source_type].items()):
            # Apply intent filtering if provided
            relevant_funcs = functions
            if intent:
                intent_lower = intent.lower()
                # Boost functions matching intent keywords
                scored = []
                for f in functions:
                    score = 1
                    if any(kw in f.lower() for kw in intent_lower.split()):
                        score = 2
                    scored.append((score, f))
                scored.sort(reverse=True)
                relevant_funcs = [f for _, f in scored]

            # Include parameter info for each function so agent knows exact param names
            func_details = []
            for fn in relevant_funcs[:3]:
                if fn in registry:
                    params = registry[fn].parameters
                    # Extract just param names and types for token efficiency
                    param_info = {k: v.get("type", "unknown") for k, v in params.items()}
                    func_details.append({"name": fn, "params": param_info})
                else:
                    func_details.append({"name": fn})

            source_nav["can_reach"].append({
                "target": target_type,
                "functions": func_details,
                "total_functions": len(functions),
            })

        if source_nav["can_reach"]:
            navigation.append(source_nav)

    return {
        "source_entities": list(source_types),
        "navigation_options": navigation,
        "total_sources": len(source_types),
        "intent": intent,
        "hint": "Use call_endpoint with one of the suggested functions. "
                "Pass entity as [namespace, id] tuple, e.g., gene=[\"HGNC\", \"6407\"]",
    }


async def call_endpoint(
    endpoint: str,
    kwargs: str,
    get_client_func: Callable,
    auto_ground: bool = True,
    disclosure_level: Optional[str] = None,
    offset: int = 0,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    """Call an autoclient endpoint with optional auto-grounding and enrichment.

    If auto_ground=True and a string is passed where a CURIE tuple is expected,
    automatically ground via GILDA.

    Parameters
    ----------
    endpoint : str
        Function name (e.g., "get_diseases_for_gene")
    kwargs : str
        JSON string of arguments. Entity args can be:
        - CURIE tuple: ["HGNC", "6407"]
        - Natural language: "LRRK2" (auto-grounded if auto_ground=True)
    get_client_func : Callable
        Function to get Neo4j client
    auto_ground : bool
        Whether to auto-ground string params to CURIEs (default: True)
    disclosure_level : str, optional
        Enrich results with metadata. Options:
        - None (default): Raw results only (most token-efficient)
        - "minimal": Pass-through (same as None)
        - "standard": Add descriptions + next steps (~250 tokens/item)
        - "detailed": Add provenance (~400 tokens/item)
        - "exploratory": Add workflows + research context (~750 tokens/item)
    offset : int
        Starting offset for pagination (default: 0)
    limit : int, optional
        Maximum items to return per page. If response exceeds ~20k tokens,
        it will be automatically truncated with has_more=True.

    Returns
    -------
    :
        Dict with endpoint, parameters, results, result_count, pagination,
        and optionally grounding_applied, enrichment, or error fields.
        If has_more=True in pagination, call again with next_offset to continue.
    """
    registry, func_mapping, _ = _get_registry()

    if endpoint not in func_mapping:
        return {
            "error": f"Unknown endpoint: {endpoint}",
            "hint": "Use suggest_endpoints to find available functions",
            "available": list(registry.keys())[:20],
        }

    # Parse kwargs
    try:
        parsed_kwargs = json.loads(kwargs) if isinstance(kwargs, str) else kwargs
    except json.JSONDecodeError as e:
        return {"error": f"Invalid JSON: {e}", "kwargs_received": kwargs}

    func = func_mapping[endpoint]
    grounding_info = {}

    # Process parameters - normalize CURIEs and auto-ground strings
    from inspect import signature
    from typing import get_args, get_origin

    try:
        func_sig = signature(func)
        for param_name, param_value in list(parsed_kwargs.items()):
            if param_name not in func_sig.parameters:
                continue

            param_type = func_sig.parameters[param_name].annotation

            # Check if parameter expects Tuple[str, str] (CURIE)
            if get_origin(param_type) is tuple and get_args(param_type) == (str, str):
                # Case 1: Already a list/tuple CURIE - normalize namespace case
                if isinstance(param_value, (list, tuple)) and len(param_value) == 2:
                    parsed_kwargs[param_name] = [param_value[0].lower(), param_value[1]]
                    continue

                # Case 2: String - try auto-grounding if enabled
                if auto_ground and isinstance(param_value, str):

                    # Ground with parameter semantics filtering
                    # param_name (disease, gene, drug) filters to appropriate namespaces
                    grounding = await ground_entity(
                        param_value,
                        limit=10,
                        param_name=param_name,  # Semantic filtering!
                    )

                    if "error" in grounding:
                        return {
                            "error": f"Could not ground '{param_value}': {grounding['error']}",
                            "parameter": param_name,
                            "hint": "Provide explicit CURIE: [namespace, id]",
                        }

                    if not grounding.get("groundings"):
                        return {
                            "error": f"No grounding found for '{param_value}' as {param_name}",
                            "parameter": param_name,
                            "namespaces_searched": grounding.get("namespaces_allowed"),
                            "hint": "Provide explicit CURIE: [namespace, id]",
                        }

                    top = grounding["top_match"]
                    top_score = top["score"]

                    # DUAL-CHECK AMBIGUITY DETECTION
                    # 1. Absolute threshold: top score must be >= MIN_CONFIDENCE_THRESHOLD
                    if top_score < MIN_CONFIDENCE_THRESHOLD:
                        return {
                            "error": f"Low confidence grounding for '{param_value}'",
                            "parameter": param_name,
                            "top_score": top_score,
                            "threshold": MIN_CONFIDENCE_THRESHOLD,
                            "grounding_options": grounding["groundings"][:5],
                            "hint": "Choose one and provide as [namespace, id]",
                        }

                    # 2. Relative clustering: no result in top 5 within AMBIGUITY_SCORE_THRESHOLD
                    if len(grounding["groundings"]) > 1:
                        top_5 = grounding["groundings"][:5]
                        for other in top_5[1:]:
                            if top_score - other["score"] < AMBIGUITY_SCORE_THRESHOLD:
                                return {
                                    "error": f"Ambiguous term '{param_value}' for {param_name}",
                                    "parameter": param_name,
                                    "reason": f"Multiple results within {AMBIGUITY_SCORE_THRESHOLD} of top score",
                                    "grounding_options": top_5,
                                    "hint": "Choose one and provide as [namespace, id]",
                                }

                    # Apply grounding - unambiguous match!
                    # IMPORTANT: Normalize namespace to lowercase for autoclient functions
                    parsed_kwargs[param_name] = [top["namespace"].lower(), top["identifier"]]
                    grounding_info[param_name] = {
                        "input": param_value,
                        "grounded_to": top,
                        "method": "gilda",
                        "param_filter": param_name,
                    }

    except Exception as e:
        logger.warning(f"Auto-grounding failed: {e}")
        # Continue without auto-grounding

    # Execute function with xref fallback
    try:
        client = get_client_func()

        # Try original parameters first
        result = await asyncio.to_thread(func, client=client, **parsed_kwargs)
        processed = process_result(result)

        # Resolve entity names from graph (batch query for all results)
        if isinstance(processed, list) and processed:
            processed = await asyncio.to_thread(resolve_entity_names, processed, client)

        # CROSS-REFERENCE FALLBACK: If no results and we auto-grounded, try xrefs
        if (isinstance(processed, list) and len(processed) == 0 and grounding_info):
            logger.info(f"No results with original grounding, trying xrefs...")

            # Try each auto-grounded parameter with its xrefs
            for param_name, ground_info in grounding_info.items():
                if param_name not in parsed_kwargs:
                    continue

                original_curie = parsed_kwargs[param_name]
                namespace, identifier = original_curie[0], original_curie[1]

                # Look up cross-references
                equivalents = _lookup_xrefs(namespace, identifier, client, param_name)

                if len(equivalents) > 1:  # More than just the original
                    logger.info(f"Trying {len(equivalents)-1} xrefs for {param_name}")
                    ground_info["xrefs_tried"] = []

                    # Try each equivalent identifier
                    for equiv_ns, equiv_id in equivalents[1:]:  # Skip the first (original)
                        test_kwargs = parsed_kwargs.copy()
                        test_kwargs[param_name] = [equiv_ns, equiv_id]

                        try:
                            xref_result = await asyncio.to_thread(func, client=client, **test_kwargs)
                            xref_processed = process_result(xref_result)

                            ground_info["xrefs_tried"].append({
                                "namespace": equiv_ns,
                                "identifier": equiv_id,
                                "result_count": len(xref_processed) if isinstance(xref_processed, list) else 1
                            })

                            if isinstance(xref_processed, list) and len(xref_processed) > 0:
                                # Found results with this xref! Resolve names.
                                logger.info(f"Found {len(xref_processed)} results using xref {equiv_ns}:{equiv_id}")
                                xref_processed = await asyncio.to_thread(resolve_entity_names, xref_processed, client)
                                processed = xref_processed
                                parsed_kwargs[param_name] = [equiv_ns, equiv_id]
                                ground_info["xref_used"] = {
                                    "namespace": equiv_ns,
                                    "identifier": equiv_id,
                                    "original_namespace": namespace,
                                    "original_identifier": identifier,
                                }
                                break
                        except Exception as e:
                            logger.debug(f"xref {equiv_ns}:{equiv_id} failed: {e}")
                            ground_info["xrefs_tried"].append({
                                "namespace": equiv_ns,
                                "identifier": equiv_id,
                                "error": str(e)
                            })

        # Optionally apply enrichment if disclosure_level specified
        enrichment_info = None
        if disclosure_level and disclosure_level != "minimal":
            try:
                from .enrichment import enrich_results, DisclosureLevel
                if isinstance(processed, list) and processed:
                    enrichment_result = await asyncio.to_thread(
                        enrich_results,
                        results=processed,
                        disclosure_level=DisclosureLevel(disclosure_level),
                        result_type=None,  # Auto-detect from results
                        client=client,
                    )
                    processed = enrichment_result["results"]
                    enrichment_info = {
                        "disclosure_level": disclosure_level,
                        "token_estimate": enrichment_result["token_estimate"],
                    }
            except ValueError as e:
                logger.warning(f"Invalid disclosure_level '{disclosure_level}': {e}")
            except Exception as e:
                logger.warning(f"Enrichment failed: {e}")

        # Generate suggested_next from result entity types
        # This embeds navigation guidance directly in the response
        suggested_next = None
        if isinstance(processed, list) and processed:
            # Extract entity IDs from results for navigation suggestions
            result_ids = []
            for item in processed[:10]:  # Sample first 10 for efficiency
                if isinstance(item, dict):
                    if "db_ns" in item and "db_id" in item:
                        result_ids.append(f"{item['db_ns']}:{item['db_id']}")
                    elif "id" in item:
                        result_ids.append(item["id"] if isinstance(item["id"], str) else f"{item['id'][0]}:{item['id'][1]}")

            if result_ids:
                nav_suggestions = suggest_endpoints(result_ids, intent=None, top_k=3)
                if "navigation_options" in nav_suggestions and nav_suggestions["navigation_options"]:
                    # Flatten to compact format: [{target, functions}]
                    suggested_next = []
                    for nav in nav_suggestions["navigation_options"]:
                        for reach in nav.get("can_reach", [])[:3]:
                            suggested_next.append({
                                "from": nav["from"],
                                "to": reach["target"],
                                "functions": reach["functions"][:2],
                            })

        # Apply pagination to results
        if isinstance(processed, list):
            paginated = paginate_response(
                processed,
                offset=offset,
                limit=limit,
                include_token_estimate=True,
            )
            final_results = paginated["results"]
            pagination_info = paginated["pagination"]
        else:
            final_results = processed
            pagination_info = {
                "total": 1,
                "offset": 0,
                "returned": 1,
                "has_more": False,
                "token_estimate": estimate_tokens(processed),
            }

        response = {
            "endpoint": endpoint,
            "parameters": parsed_kwargs,
            "results": final_results,
            "result_count": pagination_info.get("returned", len(final_results) if isinstance(final_results, list) else 1),
            "pagination": pagination_info,
        }

        # Add continuation hint if there's more data
        if pagination_info.get("has_more"):
            returned = pagination_info.get("returned", 0)
            total = pagination_info.get("total", "unknown")
            next_offset = pagination_info.get("next_offset", 0)
            response["continuation_hint"] = (
                f"Showing {returned} of {total} results. "
                f"To get more, call {endpoint} with offset={next_offset}"
            )

        # Add suggested_next if we have navigation options
        if suggested_next:
            response["suggested_next"] = suggested_next

        if grounding_info:
            response["grounding_applied"] = grounding_info

        if enrichment_info:
            response["enrichment"] = enrichment_info

        return response

    except Exception as e:
        logger.error(f"Error executing {endpoint}: {e}", exc_info=True)
        return {
            "endpoint": endpoint,
            "error": str(e),
            "parameters": parsed_kwargs,
        }


async def ground_entity(
    term: str,
    organism: Optional[str] = None,
    limit: int = 10,
    param_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Ground natural language term to CURIEs using GILDA.

    Wraps ground_biomedical_term from client.queries and adds:
    - Parameter semantics filtering (param_name → namespace filter)
    - MCP-friendly output format

    Parameters
    ----------
    term : str
        Natural language term (e.g., "LRRK2", "Parkinson's disease")
    organism : str, optional
        Organism context (e.g., "human", "9606")
    limit : int
        Max results before filtering (default: 10)
    param_name : str, optional
        Parameter name for semantic filtering (e.g., "disease", "gene", "drug")

    Returns
    -------
    :
        Dict with query, groundings, top_match, param_filter, and
        namespaces_allowed fields
    """
    try:
        from indra_cogex.client.queries import ground_biomedical_term

        # Reuse existing grounding function (handles gilda lib + HTTP fallback)
        raw_results = ground_biomedical_term(term, organism=organism, limit=limit)

        # Get namespace filter if param_name provided
        allowed_namespaces = None
        if param_name:
            allowed_namespaces = PARAM_NAMESPACE_FILTERS.get(param_name.lower())

        # Convert to MCP format and apply namespace filtering
        groundings = []
        for result in raw_results:
            term_info = result.get("term", {})
            namespace = term_info.get("db", "").lower()

            # Apply namespace filter if specified
            if allowed_namespaces and namespace not in allowed_namespaces:
                continue

            groundings.append({
                "curie": f"{term_info.get('db', '')}:{term_info.get('id', '')}",
                "namespace": term_info.get("db", ""),
                "identifier": term_info.get("id", ""),
                "name": term_info.get("entry_name", ""),
                "score": round(result.get("score", 0), 3),
                "source": term_info.get("source", ""),
            })

        return {
            "query": term,
            "groundings": groundings,
            "top_match": groundings[0] if groundings else None,
            "param_filter": param_name,
            "namespaces_allowed": list(allowed_namespaces) if allowed_namespaces else None,
        }
    except ImportError as e:
        return {
            "error": f"Grounding dependencies not available: {e}",
            "hint": "Install gilda: pip install gilda",
        }
    except Exception as e:
        return {"error": f"Grounding failed: {e}", "query": term}


def get_navigation_schema() -> Dict[str, Any]:
    """Get the full navigation schema (edge map) for discovery.

    Returns all possible navigation paths in the knowledge graph
    as extracted from function signatures, including parameter info.
    """
    registry, _, edge_map = _get_registry()

    schema = {
        "entity_types": sorted(set(
            list(edge_map.keys()) +
            [t for targets in edge_map.values() for t in targets.keys()]
        )),
        "edges": [],
    }

    for source, targets in sorted(edge_map.items()):
        for target, functions in sorted(targets.items()):
            # Include parameter info so agent knows exact param names
            func_details = []
            for fn in functions:
                if fn in registry:
                    params = registry[fn].parameters
                    param_info = {k: v.get("type", "unknown") for k, v in params.items()}
                    func_details.append({"name": fn, "params": param_info})
                else:
                    func_details.append({"name": fn})

            schema["edges"].append({
                "from": source,
                "to": target,
                "functions": func_details,
                "count": len(functions),
            })

    return schema


def register_gateway_tools(mcp, get_client_func: Callable) -> int:
    """Register gateway tools on MCP server.

    Only 4 tools instead of 100+:
    1. ground_entity - Natural language → CURIEs
    2. suggest_endpoints - Graph navigation suggestions
    3. call_endpoint - Execute any autoclient function
    4. get_navigation_schema - Full edge map for discovery

    Parameters
    ----------
    mcp : FastMCP
        MCP server instance
    get_client_func : Callable
        Function to get Neo4j client

    Returns
    -------
    :
        Number of tools registered (always 4)
    """
    # Pre-build registry
    _get_registry()

    @mcp.tool(
        name="ground_entity",
        annotations={"title": "Ground Entity (GILDA)", "readOnlyHint": True}
    )
    async def ground_entity_tool(
        term: str,
        organism: Optional[str] = None,
        limit: int = 10,
        param_name: Optional[str] = None,
    ) -> str:
        """Ground natural language to CURIEs using GILDA with semantic filtering.

        Parameter semantics eliminate cross-type ambiguity:
        - disease="ALS" → filters to disease namespaces → MESH:D000690 (not SOD1 gene)
        - gene="ALS" → filters to gene namespaces → HGNC:396 (SOD1)
        - drug="aspirin" → filters to drug namespaces → CHEBI:15365

        Parameters
        ----------
        term : str
            Natural language term (e.g., "LRRK2", "Parkinson's disease")
        organism : str, optional
            Organism context (e.g., "human")
        limit : int
            Max results (default: 10)
        param_name : str, optional
            Parameter type for semantic filtering: disease, gene, drug, pathway, etc.
            Filters results to appropriate namespaces for that entity type.
        """
        result = await ground_entity(term, organism, limit, param_name)
        return json.dumps(result, indent=2, default=str)

    @mcp.tool(
        name="suggest_endpoints",
        annotations={"title": "Suggest Navigation", "readOnlyHint": True}
    )
    async def suggest_endpoints_tool(
        entity_ids: List[str],
        intent: Optional[str] = None,
        top_k: int = 10,
    ) -> str:
        """Suggest where to navigate next in the knowledge graph.

        Given entity CURIEs you have, shows what entity types you can reach
        and which functions traverse those edges.

        Parameters
        ----------
        entity_ids : List[str]
            CURIEs from previous results (e.g., ["HGNC:6407"])
        intent : str, optional
            Exploration goal (e.g., "find drug targets")
        top_k : int
            Max suggestions per entity type
        """
        result = suggest_endpoints(entity_ids, intent, top_k)
        return json.dumps(result, indent=2, default=str)

    @mcp.tool(
        name="call_endpoint",
        annotations={"title": "Call Endpoint", "readOnlyHint": True}
    )
    async def call_endpoint_tool(
        endpoint: str,
        kwargs: str,
        auto_ground: bool = True,
        disclosure_level: Optional[str] = None,
        offset: int = 0,
        limit: Optional[int] = None,
    ) -> str:
        """Call any autoclient endpoint with optional auto-grounding.

        Parameters
        ----------
        endpoint : str
            Function name (e.g., "get_diseases_for_gene")
        kwargs : str
            JSON arguments. Entities can be:
            - CURIE tuple: {"gene": ["HGNC", "6407"]}
            - Natural language: {"gene": "LRRK2"} (auto-grounded)
        auto_ground : bool
            Auto-ground strings to CURIEs (default: True)
        disclosure_level : str, optional
            Enrich results with metadata. Options:
            - None (default): Raw results with names (most efficient)
            - "standard": Add descriptions + suggested next steps
            - "detailed": Add provenance metadata
            - "exploratory": Add workflows + research context
        offset : int
            Starting offset for pagination (default: 0). Use next_offset
            from previous response to continue fetching.
        limit : int, optional
            Max items per page. Responses are auto-truncated to ~20k tokens.
        """
        result = await call_endpoint(
            endpoint, kwargs, get_client_func, auto_ground, disclosure_level,
            offset=offset, limit=limit
        )
        return json.dumps(result, indent=2, default=str)

    @mcp.tool(
        name="get_navigation_schema",
        annotations={"title": "Get Navigation Schema", "readOnlyHint": True}
    )
    async def get_navigation_schema_tool() -> str:
        """Get full navigation schema showing all entity types and edges.

        Returns the complete map of how entity types connect in the
        knowledge graph, extracted from function signatures.
        """
        result = get_navigation_schema()
        return json.dumps(result, indent=2, default=str)

    logger.info("Registered 4 gateway tools: ground_entity, suggest_endpoints, "
                "call_endpoint, get_navigation_schema")
    return 4


__all__ = [
    "register_gateway_tools",
    "suggest_endpoints",
    "call_endpoint",
    "ground_entity",
    "get_navigation_schema",
    "clear_registry_cache",
]
