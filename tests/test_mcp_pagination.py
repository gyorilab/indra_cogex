"""Tests for token-aware pagination module.

Tests cover the core pagination behaviors:
1. Token estimation accuracy
2. Offset/limit pagination
3. Automatic token truncation with binary search
4. Edge cases (empty lists, single items, oversized items)
"""

import unittest

from indra_cogex.client.pagination import (
    estimate_tokens,
    paginate_list,
    truncate_to_token_limit,
    paginate_response,
    PaginationConfig,
)


class TestTokenEstimation(unittest.TestCase):
    """Test token estimation heuristics."""

    def test_empty_list(self):
        """Empty list should have minimal tokens."""
        tokens = estimate_tokens([])
        self.assertLess(tokens, 10)

    def test_simple_dict(self):
        """Simple dict token count should scale with content."""
        small = {"a": 1}
        large = {"a": 1, "b": 2, "c": 3, "d": "a longer string value"}
        self.assertLess(estimate_tokens(small), estimate_tokens(large))

    def test_list_overhead(self):
        """List should add per-item overhead."""
        items = [{"id": i} for i in range(10)]
        tokens = estimate_tokens(items)
        # Should be roughly 10 items * ~10 chars each / 4 + overhead
        self.assertGreater(tokens, 20)
        self.assertLess(tokens, 200)


class TestPaginateList(unittest.TestCase):
    """Test offset/limit pagination."""

    def test_basic_pagination(self):
        """Paginate with offset and limit."""
        items = list(range(100))
        page, meta = paginate_list(items, offset=10, limit=20)

        self.assertEqual(len(page), 20)
        self.assertEqual(page[0], 10)
        self.assertEqual(page[-1], 29)
        self.assertEqual(meta["total"], 100)
        self.assertEqual(meta["offset"], 10)
        self.assertEqual(meta["returned"], 20)
        self.assertTrue(meta["has_more"])
        self.assertEqual(meta["next_offset"], 30)

    def test_last_page(self):
        """Last page should have has_more=False."""
        items = list(range(25))
        page, meta = paginate_list(items, offset=20, limit=10)

        self.assertEqual(len(page), 5)
        self.assertFalse(meta["has_more"])
        self.assertNotIn("next_offset", meta)

    def test_offset_beyond_total(self):
        """Offset past end returns empty list."""
        items = list(range(10))
        page, meta = paginate_list(items, offset=100, limit=10)

        self.assertEqual(len(page), 0)
        self.assertEqual(meta["returned"], 0)
        self.assertFalse(meta["has_more"])


class TestTokenTruncation(unittest.TestCase):
    """Test automatic token-based truncation."""

    def test_no_truncation_needed(self):
        """Small list should not be truncated."""
        items = [{"id": i} for i in range(5)]
        result, meta = truncate_to_token_limit(items, max_tokens=10000)

        self.assertEqual(len(result), 5)
        self.assertFalse(meta["truncated"])

    def test_truncation_binary_search(self):
        """Large list should be truncated via binary search."""
        # Create items that will exceed token limit
        items = [{"id": i, "data": "x" * 100} for i in range(1000)]
        result, meta = truncate_to_token_limit(items, max_tokens=1000)

        self.assertLess(len(result), 1000)
        self.assertTrue(meta["truncated"])
        self.assertTrue(meta["has_more"])
        self.assertIn("next_offset", meta)
        self.assertLessEqual(meta["token_estimate"], 1000)

    def test_empty_list(self):
        """Empty list returns empty with no truncation."""
        result, meta = truncate_to_token_limit([], max_tokens=1000)

        self.assertEqual(result, [])
        self.assertFalse(meta["truncated"])


class TestPaginateResponse(unittest.TestCase):
    """Test combined pagination with token limiting."""

    def test_list_pagination(self):
        """List should be paginated with metadata."""
        items = list(range(100))
        result = paginate_response(items, offset=0, limit=10)

        self.assertEqual(len(result["results"]), 10)
        self.assertEqual(result["pagination"]["total"], 100)
        self.assertTrue(result["pagination"]["has_more"])
        self.assertIn("token_estimate", result["pagination"])

    def test_dict_with_results_key(self):
        """Dict with 'results' key should paginate results."""
        data = {
            "results": list(range(50)),
            "metadata": {"source": "test"},
        }
        result = paginate_response(data, offset=0, limit=10)

        self.assertEqual(len(result["results"]), 10)
        self.assertEqual(result["metadata"], {"source": "test"})
        self.assertIn("pagination", result)

    def test_token_limit_overrides_page_limit(self):
        """Token limit should further truncate if page exceeds tokens."""
        # Large items that exceed token limit even with small page
        items = [{"id": i, "payload": "x" * 500} for i in range(100)]
        config = PaginationConfig(max_tokens=500)

        result = paginate_response(items, offset=0, limit=50, config=config)

        # Should be truncated below limit=50 due to token constraint
        self.assertLess(len(result["results"]), 50)
        self.assertTrue(result["pagination"]["has_more"])


if __name__ == "__main__":
    unittest.main()
