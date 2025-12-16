"""Function registry and edge map cache for MCP gateway."""

import logging
import re
import threading
from collections import defaultdict
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from .mappings import ENTITY_TYPE_MAPPINGS

logger = logging.getLogger(__name__)

# Module-level registry and edge map cache
#
# Cache Management Strategy:
# - Lazy initialization on first use (_get_registry)
# - Validation before returning cached values (_validate_cached_registry)
# - Automatic rebuild on corruption detection (e.g., 'dict' object not callable)
# - Thread-safe via RLock for concurrent MCP requests
# - Version tracking for diagnostics (clear_registry_cache increments)
# - Hot-reload support: call clear_registry_cache() when queries_web code changes
#
_FUNCTION_REGISTRY: Optional[Dict[str, Any]] = None
_FUNC_MAPPING: Optional[Dict[str, Callable]] = None
_EDGE_MAP: Optional[Dict[str, Dict[str, List[str]]]] = None  # source → target → [functions]

# Cache metadata for diagnostics and staleness detection
_CACHE_VERSION: int = 0
_CACHE_INITIALIZED_AT: Optional[datetime] = None

# Thread lock for cache operations (reentrant to allow same-thread recursive calls)
_cache_lock = threading.RLock()


def _validate_cached_registry() -> bool:
    """Validate that cached functions are actually callable.

    Samples random functions from the cache to detect corruption like
    non-callable objects that can occur during hot-reload scenarios.

    Returns
    -------
    :
        True if cache is valid, False if corrupted
    """
    global _FUNCTION_REGISTRY, _FUNC_MAPPING, _EDGE_MAP

    try:
        # Check that basic structures exist and are correct types
        if not isinstance(_FUNCTION_REGISTRY, dict):
            logger.error(f"Registry corruption: _FUNCTION_REGISTRY is {type(_FUNCTION_REGISTRY)}, expected dict")
            return False

        if not isinstance(_FUNC_MAPPING, dict):
            logger.error(f"Registry corruption: _FUNC_MAPPING is {type(_FUNC_MAPPING)}, expected dict")
            return False

        if not isinstance(_EDGE_MAP, dict):
            logger.error(f"Registry corruption: _EDGE_MAP is {type(_EDGE_MAP)}, expected dict")
            return False

        # Sample validation: check that a few random functions from func_mapping are callable
        import random
        if _FUNC_MAPPING:
            sample_size = min(5, len(_FUNC_MAPPING))
            sample_funcs = random.sample(list(_FUNC_MAPPING.items()), sample_size)

            for func_name, func in sample_funcs:
                if not callable(func):
                    logger.error(f"Registry corruption: {func_name} is {type(func)}, expected callable")
                    return False

        logger.debug("Cache validation passed")
        return True

    except Exception as e:
        logger.error(f"Cache validation error: {e}")
        return False


def _clear_registry_internal():
    """Internal function to clear registry cache (assumes lock is held)."""
    global _FUNCTION_REGISTRY, _FUNC_MAPPING, _EDGE_MAP

    _FUNCTION_REGISTRY = None
    _FUNC_MAPPING = None
    _EDGE_MAP = None
    logger.debug("Registry cache cleared (internal)")


def invalidate_cache():
    """Invalidate and clear the function registry cache.

    Thread-safe cache invalidation. Next call to _get_registry() will rebuild.
    Alias for clear_registry_cache() for backward compatibility.
    """
    clear_registry_cache()


def clear_registry_cache() -> Dict[str, Any]:
    """Clear the function registry cache.

    Thread-safe cache invalidation. Next call to _get_registry() will rebuild.
    Use this when queries_web code has been modified while server is running.

    Returns
    -------
    :
        Dict with status, was_cached, previous_version, and timestamp
    """
    global _FUNCTION_REGISTRY, _FUNC_MAPPING, _EDGE_MAP, _CACHE_VERSION

    with _cache_lock:
        was_cached = _FUNCTION_REGISTRY is not None
        old_version = _CACHE_VERSION

        _clear_registry_internal()

        logger.info(f"Registry cache cleared (was v{old_version}, cached={was_cached})")

        return {
            "status": "cleared",
            "was_cached": was_cached,
            "previous_version": old_version,
            "timestamp": datetime.now().isoformat(),
        }


def get_registry_status() -> Dict[str, Any]:
    """Get detailed status of registry cache for diagnostics.

    Thread-safe read of cache metadata without triggering initialization.
    Useful for debugging cache corruption or staleness issues.

    Returns
    -------
    :
        Dict with cached, version, initialized_at, age_seconds, metrics,
        validation, and status fields
    """
    global _FUNCTION_REGISTRY, _FUNC_MAPPING, _EDGE_MAP, _CACHE_VERSION, _CACHE_INITIALIZED_AT

    with _cache_lock:
        is_cached = _FUNCTION_REGISTRY is not None

        if not is_cached:
            return {
                "cached": False,
                "version": _CACHE_VERSION,
                "status": "not initialized",
            }

        # Get cache metrics
        num_functions = len(_FUNCTION_REGISTRY) if _FUNCTION_REGISTRY else 0
        num_mappings = len(_FUNC_MAPPING) if _FUNC_MAPPING else 0
        num_edges = sum(len(targets) for targets in _EDGE_MAP.values()) if _EDGE_MAP else 0

        # Sample function validation
        sample_valid = True
        validation_error = None
        try:
            if _FUNC_MAPPING:
                import random
                sample = random.choice(list(_FUNC_MAPPING.items()))
                func_name, func = sample
                if not callable(func):
                    sample_valid = False
                    validation_error = f"{func_name} is {type(func)}, not callable"
        except Exception as e:
            sample_valid = False
            validation_error = str(e)

        # Time since initialization
        age_seconds = None
        if _CACHE_INITIALIZED_AT:
            age_seconds = (datetime.now() - _CACHE_INITIALIZED_AT).total_seconds()

        return {
            "cached": True,
            "version": _CACHE_VERSION,
            "initialized_at": _CACHE_INITIALIZED_AT.isoformat() if _CACHE_INITIALIZED_AT else None,
            "age_seconds": age_seconds,
            "metrics": {
                "registry_functions": num_functions,
                "func_mapping_entries": num_mappings,
                "navigation_edges": num_edges,
            },
            "validation": {
                "sample_check_passed": sample_valid,
                "error": validation_error,
            },
            "status": "healthy" if sample_valid else "corrupted",
        }


def _get_registry():
    """Lazily build and cache the function registry and edge map.

    Thread-safe lazy initialization with cache validation.
    """
    global _FUNCTION_REGISTRY, _FUNC_MAPPING, _EDGE_MAP, _CACHE_VERSION, _CACHE_INITIALIZED_AT

    with _cache_lock:
        # Validate cache before returning
        if _FUNCTION_REGISTRY is not None:
            # Validate that cached functions are actually callable
            if not _validate_cached_registry():
                logger.warning("Cache validation failed - rebuilding registry")
                _clear_registry_internal()

        if _FUNCTION_REGISTRY is not None:
            return _FUNCTION_REGISTRY, _FUNC_MAPPING, _EDGE_MAP

        logger.info("Building function registry and edge map...")

        try:
            from indra_cogex.apps.queries_web import (
                FUNCTION_CATEGORIES,
                CATEGORY_DESCRIPTIONS,
                func_mapping,
                module_functions,
                examples_dict,
                SKIP_GLOBAL,
                SKIP_ARGUMENTS,
            )
            from indra_cogex.apps.queries_web.helpers import get_docstring
            from indra_cogex.apps.queries_web.introspection import build_function_registry
        except ImportError as e:
            logger.warning(f"Could not import queries_web: {e}")
            _FUNCTION_REGISTRY = {}
            _FUNC_MAPPING = {}
            _EDGE_MAP = {}
            _CACHE_INITIALIZED_AT = datetime.now()
            return _FUNCTION_REGISTRY, _FUNC_MAPPING, _EDGE_MAP

        # Build function registry
        _FUNCTION_REGISTRY = build_function_registry(
            module_functions=module_functions,
            func_mapping=func_mapping,
            function_categories=FUNCTION_CATEGORIES,
            category_descriptions=CATEGORY_DESCRIPTIONS,
            examples_dict=examples_dict,
            skip_global=SKIP_GLOBAL,
            skip_arguments=SKIP_ARGUMENTS,
            get_docstring_func=get_docstring,
        )
        _FUNC_MAPPING = func_mapping

        # Build edge map from function naming patterns
        _EDGE_MAP = _build_edge_map(_FUNCTION_REGISTRY)

        # Update cache metadata
        _CACHE_VERSION += 1
        _CACHE_INITIALIZED_AT = datetime.now()

        logger.info(f"Built function registry v{_CACHE_VERSION} with {len(_FUNCTION_REGISTRY)} functions, "
                    f"{sum(len(targets) for targets in _EDGE_MAP.values())} navigation edges")
        return _FUNCTION_REGISTRY, _FUNC_MAPPING, _EDGE_MAP


def _build_edge_map(registry: Dict[str, Any]) -> Dict[str, Dict[str, List[str]]]:
    """Build edge map from function naming patterns.

    Pattern analysis (from schema_builder approach):
    - get_X_for_Y → edge: Y → X (navigate from Y to X)
    - get_Xs_for_Y → edge: Y → X (plural)
    - is_X_in_Y → edge: X ∈ Y (membership check)
    - has_X_Y_association → edge: X ↔ Y (bidirectional)

    Returns
    -------
    :
        Nested dict: source_entity → target_entity → [function_names]
    """
    edge_map: Dict[str, Dict[str, List[str]]] = defaultdict(lambda: defaultdict(list))

    # Regex patterns for function name analysis
    get_for_pattern = re.compile(r'get_(\w+?)s?_for_(\w+?)s?$')
    is_in_pattern = re.compile(r'is_(\w+)_in_(\w+)')
    has_assoc_pattern = re.compile(r'has_(\w+)_(\w+)_association')

    for func_name, metadata in registry.items():
        # Try get_X_for_Y pattern (most common)
        match = get_for_pattern.match(func_name)
        if match:
            target_param = match.group(1)  # X (what we get)
            source_param = match.group(2)  # Y (what we have)

            target_type = ENTITY_TYPE_MAPPINGS.get(target_param.lower())
            source_type = ENTITY_TYPE_MAPPINGS.get(source_param.lower())

            if target_type and source_type:
                edge_map[source_type][target_type].append(func_name)
            continue

        # Try is_X_in_Y pattern
        match = is_in_pattern.match(func_name)
        if match:
            x_param = match.group(1)
            y_param = match.group(2)

            x_type = ENTITY_TYPE_MAPPINGS.get(x_param.lower())
            y_type = ENTITY_TYPE_MAPPINGS.get(y_param.lower())

            if x_type and y_type:
                # Both directions for membership checks
                edge_map[x_type][y_type].append(func_name)
            continue

        # Try has_X_Y_association pattern
        match = has_assoc_pattern.match(func_name)
        if match:
            x_param = match.group(1)
            y_param = match.group(2)

            x_type = ENTITY_TYPE_MAPPINGS.get(x_param.lower())
            y_type = ENTITY_TYPE_MAPPINGS.get(y_param.lower())

            if x_type and y_type:
                # Bidirectional for associations
                edge_map[x_type][y_type].append(func_name)
                edge_map[y_type][x_type].append(func_name)

    # Convert defaultdicts to regular dicts
    return {source: dict(targets) for source, targets in edge_map.items()}


__all__ = [
    "invalidate_cache",
    "clear_registry_cache",
    "get_registry_status",
    "_get_registry",
    "_build_edge_map",
]
