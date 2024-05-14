# -*- coding: utf-8 -*-

"""Tests for the build CLI."""

import json
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterable, Optional, Type
from unittest import mock

import click
from click.testing import CliRunner

from indra_cogex.representation import Node, Relation
from indra_cogex.sources.cli import main
from indra_cogex.sources.processor import Processor

SUCCESS_TEXT = "mock successful"
DEFAULT_KEY = "default"


class MockProcessor(Processor):
    """A mock processor."""

    name = "mock_processor"
    node_types = ["mock_node"]

    def __init__(self, key: Optional[str] = None):
        self.key = key or DEFAULT_KEY
        click.echo(SUCCESS_TEXT)
        click.echo(self.key)

    def get_nodes(self):
        """Return mock nodes for testing."""
        return [Node(db_ns="HGNC", db_id="123", labels=["BioEntity"]),
                Node(db_ns="HGNC", db_id="456", labels=["BioEntity"])]

    def get_relations(self):
        """Return a mock relation for testing."""
        return [Relation(source_ns="HGNC", source_id="123", target_ns="HGNC",
                         target_id="456", rel_type="mock_relation"), ]


def _new_iter() -> Iterable[Type[Processor]]:
    yield MockProcessor


#: This patch allows for switching out the normal class resolver for
#: a test set of resolvers
processor_mock = mock.patch(
    "indra_cogex.sources.cli._iter_processors", side_effect=_new_iter
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

            res = runner.invoke(main, args=["--process", "--assemble"])
            self.assertEqual(0, res.exit_code, msg=res.exception)
            self.assertIn(SUCCESS_TEXT, res.output)
            self.assertIn(DEFAULT_KEY, res.output)

    @processor_mock
    def test_cli_configged(self, _):
        """Test running the CLI with a config file."""
        runner = CliRunner()
        non_default_key = "amazing"

        with TemporaryDirectory() as directory:
            directory = Path(directory)
            processor_dir = directory / "output"
            processor_dir.mkdir()

            config = {MockProcessor.name: {"key": non_default_key}}
            config_path = os.path.join(directory, "config.json")
            with open(config_path, "w") as file:
                json.dump(config, file, indent=2)

            # override the __init_subclass__ with the directory for this test
            MockProcessor.directory = directory
            MockProcessor.nodes_path = directory / "nodes.tsv.gz"
            MockProcessor.nodes_indra_path = directory / "nodes.pkl"
            MockProcessor.edges_path = directory / "edges.tsv.gz"

            res = runner.invoke(
                main,
                args=[
                    "--process",
                    "--assemble",
                    "--config",
                    config_path,
                ],
            )
            self.assertEqual(0, res.exit_code, msg=res.exception)
            self.assertIn(SUCCESS_TEXT, res.output)
            self.assertIn(non_default_key, res.output)
