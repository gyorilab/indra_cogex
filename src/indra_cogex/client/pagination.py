"""Token-aware pagination utilities for CoGEx responses.

This module provides reusable pagination primitives that combine traditional
offset/limit pagination with token-aware truncation. This is useful for any
CoGEx consumer that needs to:

1. Paginate large result sets efficiently
2. Stay within token limits (for LLM contexts, API responses, etc.)
3. Provide progressive disclosure of data

The core approach:
- Use offset/limit for user-controlled pagination
- Apply token-aware truncation when response exceeds limits
- Provide metadata for fetching continuation pages

Token estimation uses a simple heuristic (characters/4) with structural overhead.
This works well for JSON responses and is fast to compute.

Example:
    from indra_cogex.client.pagination import paginate_response

    # Paginate with token awareness
    result = paginate_response(
        data=large_list,
        offset=0,
        limit=100,
        max_tokens=20000
    )
    # Returns: {"results": [...], "pagination": {"total": ..., "has_more": ...}}

    # Continue fetching
    if result["pagination"]["has_more"]:
        next_result = paginate_response(
            data=large_list,
            offset=result["pagination"]["next_offset"],
            limit=100
        )
"""

import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Default limits
DEFAULT_MAX_TOKENS = 20000  # Leave buffer below 25k MCP limit
DEFAULT_MAX_CHARS = 80000   # ~20k tokens at 4 chars/token
DEFAULT_PAGE_SIZE = 50      # Default items per page
MAX_PAGE_SIZE = 10000       # Maximum items per page


@dataclass
class PaginationConfig:
    """Configuration for response pagination.

    Attributes
    ----------
    max_tokens : int
        Maximum tokens per response (default: 20,000)
    max_chars : int
        Maximum characters per response (default: 80,000)
    default_page_size : int
        Default number of items per page (default: 50)
    chars_per_token : float
        Heuristic for token estimation (default: 4.0)
    """
    max_tokens: int = DEFAULT_MAX_TOKENS
    max_chars: int = DEFAULT_MAX_CHARS
    default_page_size: int = DEFAULT_PAGE_SIZE
    chars_per_token: float = 4.0


def estimate_tokens(obj: Any, chars_per_token: float = 4.0) -> int:
    """Estimate token count for a JSON-serializable object.

    Uses character count / 4 as a rough heuristic, plus overhead
    for JSON structure (brackets, commas, quotes).

    Parameters
    ----------
    obj : Any
        JSON-serializable object
    chars_per_token : float
        Average characters per token (default: 4.0)

    Returns
    -------
    int
        Estimated token count
    """
    try:
        # Compact JSON for accurate character count
        json_str = json.dumps(obj, separators=(',', ':'), default=str)
        char_count = len(json_str)

        # Base token estimate
        token_estimate = int(char_count / chars_per_token)

        # Add overhead for structural tokens
        if isinstance(obj, list):
            token_estimate += len(obj) * 2  # Overhead per item
        elif isinstance(obj, dict):
            token_estimate += len(obj) * 3  # Key tokens

        return token_estimate

    except Exception as e:
        logger.warning(f"Token estimation failed: {e}")
        return len(str(obj)) // 4


def paginate_list(
    items: List[Any],
    offset: int = 0,
    limit: Optional[int] = None,
    config: Optional[PaginationConfig] = None,
) -> Tuple[List[Any], Dict[str, Any]]:
    """Paginate a list of items with offset/limit.

    Parameters
    ----------
    items : List[Any]
        Full list of items
    offset : int
        Starting index (0-based)
    limit : int, optional
        Max items to return (None = use config default)
    config : PaginationConfig, optional
        Pagination configuration

    Returns
    -------
    Tuple[List[Any], Dict[str, Any]]
        Tuple of (paginated items, pagination metadata)
    """
    if config is None:
        config = PaginationConfig()

    if limit is None:
        limit = config.default_page_size

    total = len(items)
    start = min(offset, total)
    end = min(start + limit, total)

    page_items = items[start:end]

    pagination = {
        "total": total,
        "offset": start,
        "limit": limit,
        "returned": len(page_items),
        "has_more": end < total,
    }

    if end < total:
        pagination["next_offset"] = end

    return page_items, pagination


def truncate_to_token_limit(
    items: List[Any],
    max_tokens: int = DEFAULT_MAX_TOKENS,
    config: Optional[PaginationConfig] = None,
) -> Tuple[List[Any], Dict[str, Any]]:
    """Truncate list to fit within token limit.

    Uses binary search to find the optimal truncation point.

    Parameters
    ----------
    items : List[Any]
        List of items to truncate
    max_tokens : int
        Maximum tokens allowed
    config : PaginationConfig, optional
        Pagination configuration

    Returns
    -------
    Tuple[List[Any], Dict[str, Any]]
        Tuple of (truncated items, truncation metadata)
    """
    if config is None:
        config = PaginationConfig()

    if not items:
        return items, {"truncated": False, "total": 0}

    total = len(items)

    # Check if full list fits
    full_tokens = estimate_tokens(items, config.chars_per_token)
    if full_tokens <= max_tokens:
        return items, {
            "truncated": False,
            "total": total,
            "token_estimate": full_tokens,
        }

    # Binary search for optimal truncation point
    low, high = 1, total
    best_count = 1

    while low <= high:
        mid = (low + high) // 2
        subset = items[:mid]
        tokens = estimate_tokens(subset, config.chars_per_token)

        if tokens <= max_tokens:
            best_count = mid
            low = mid + 1
        else:
            high = mid - 1

    truncated_items = items[:best_count]
    truncated_tokens = estimate_tokens(truncated_items, config.chars_per_token)

    return truncated_items, {
        "truncated": True,
        "total": total,
        "returned": best_count,
        "token_estimate": truncated_tokens,
        "next_offset": best_count,
        "has_more": best_count < total,
    }


def paginate_response(
    data: Any,
    offset: int = 0,
    limit: Optional[int] = None,
    max_tokens: Optional[int] = None,
    config: Optional[PaginationConfig] = None,
    include_token_estimate: bool = True,
) -> Dict[str, Any]:
    """Paginate any response with automatic token limiting.

    Combines offset/limit pagination with automatic token truncation.
    This is the primary entry point for most pagination needs.

    Parameters
    ----------
    data : Any
        Response data (list or dict with list values)
    offset : int
        Starting offset for pagination
    limit : int, optional
        Max items per page
    max_tokens : int, optional
        Token limit (uses config default if not specified)
    config : PaginationConfig, optional
        Pagination configuration
    include_token_estimate : bool
        Include token estimate in response

    Returns
    -------
    Dict[str, Any]
        Dict with paginated data and pagination metadata
    """
    if config is None:
        config = PaginationConfig()

    if max_tokens is None:
        max_tokens = config.max_tokens

    # Handle list data directly
    if isinstance(data, list):
        # Apply offset/limit pagination first
        page_items, pagination = paginate_list(
            data, offset=offset, limit=limit, config=config
        )

        # Then apply token truncation if needed
        final_items, truncation = truncate_to_token_limit(
            page_items, max_tokens=max_tokens, config=config
        )

        # Merge pagination info
        if truncation.get("truncated"):
            # Adjust pagination to reflect token truncation
            pagination["returned"] = truncation["returned"]
            pagination["has_more"] = True
            pagination["next_offset"] = offset + truncation["returned"]
            pagination["truncation_reason"] = "token_limit"

        if include_token_estimate:
            pagination["token_estimate"] = truncation.get(
                "token_estimate",
                estimate_tokens(final_items, config.chars_per_token)
            )

        return {
            "results": final_items,
            "pagination": pagination,
        }

    # Handle dict data with "results" key
    if isinstance(data, dict):
        results = data.get("results", [])
        if isinstance(results, list):
            paginated = paginate_response(
                results,
                offset=offset,
                limit=limit,
                max_tokens=max_tokens,
                config=config,
                include_token_estimate=include_token_estimate,
            )
            # Preserve other keys from original dict
            output = {k: v for k, v in data.items() if k != "results"}
            output["results"] = paginated["results"]
            output["pagination"] = paginated["pagination"]
            return output

        # Non-list results - just add token estimate
        if include_token_estimate:
            data["token_estimate"] = estimate_tokens(data, config.chars_per_token)
        return data

    # Primitive or other type - return as-is with token estimate
    result = {"data": data}
    if include_token_estimate:
        result["token_estimate"] = estimate_tokens(data, config.chars_per_token)
    return result


__all__ = [
    "PaginationConfig",
    "estimate_tokens",
    "paginate_list",
    "truncate_to_token_limit",
    "paginate_response",
    "DEFAULT_MAX_TOKENS",
    "DEFAULT_MAX_CHARS",
    "DEFAULT_PAGE_SIZE",
    "MAX_PAGE_SIZE",
]
