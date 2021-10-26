# -*- coding: utf-8 -*-

"""Tests for the build CLI."""

import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterable, Optional, Type
from unittest import mock

import click
from click.testing import CliRunner

from indra_cogex.sources.cli import main
from indra_cogex.sources.processor import Processor

SUCCESS_TEXT = "mock successful"
DEFAULT_KEY = "default"


class MockProcessor(Processor):
    """A mock processor."""

    name = "mock_processor"

    def __init__(self, key: Optional[str] = None):
        self.key = key or DEFAULT_KEY
        click.echo(SUCCESS_TEXT)
        click.echo(self.key)

    def get_nodes(self):
        """Return an empty sequence of nodes for testing."""
        return []

    def get_relations(self):
        """Return an empty sequence of relations for testing."""
        return []


def _new_iter() -> Iterable[Type[Processor]]:
    yield MockProcessor


#: This patch allows for switching out the normal class resolver for
#: a test set of resolvers
processor_mock = mock.patch(
    "indra_cogex.sources.cli._iter_resolvers", side_effect=_new_iter
)


class TestCLI(unittest.TestCase):
    """A test case for the build CLI."""

    @processor_mock
    def test_cli(self, _):
        """Test running the CLI."""
        runner = CliRunner()
        with TemporaryDirectory() as directory:
            directory = Path(directory)
            processor_dir = directory / "output"
            processor_dir.mkdir()

            # override the __init_subclass__ with the directory for this test
            MockProcessor.directory = directory
            MockProcessor.nodes_path = directory / "nodes.tsv.gz"
            MockProcessor.nodes_indra_path = directory / "nodes.pkl"
            MockProcessor.edges_path = directory / "edges.tsv.gz"

            res = runner.invoke(
                main, args=["--nodes-path", os.path.join(directory, "nodes.tsv")]
            )
            self.assertEqual(0, res.exit_code, msg=res.exception)
            self.assertIn(SUCCESS_TEXT, res.output)
            self.assertIn(DEFAULT_KEY, res.output)

    @processor_mock
    def test_cli_configged(self, _):
        """Test running the CLI with a config file."""
        runner = CliRunner()
        with TemporaryDirectory() as directory:
            directory = Path(directory)
            processor_dir = directory / "output"
            processor_dir.mkdir()

            config = {"key": "amazing"}

            # override the __init_subclass__ with the directory for this test
            MockProcessor.directory = directory
            MockProcessor.nodes_path = directory / "nodes.tsv.gz"
            MockProcessor.nodes_indra_path = directory / "nodes.pkl"
            MockProcessor.edges_path = directory / "edges.tsv.gz"

            res = runner.invoke(
                main, args=["--nodes-path", os.path.join(directory, "nodes.tsv")]
            )
            self.assertEqual(0, res.exit_code, msg=res.exception)
            self.assertIn(SUCCESS_TEXT, res.output)
            self.assertIn(DEFAULT_KEY, res.output)
