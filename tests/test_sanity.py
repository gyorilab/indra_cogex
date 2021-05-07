# -*- coding: utf-8 -*-

"""Sanity checks for testing."""

import unittest


class TestSanity(unittest.TestCase):
    """A trivial test case."""

    def test_sanity(self):
        """Run a trivial test."""
        self.assertIsNone(None)
