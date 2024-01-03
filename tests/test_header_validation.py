from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from indra_cogex.sources import Processor
from indra_cogex.representation import Node, Relation
from indra_cogex.sources.processor import validate_headers
from indra_cogex.sources.processor_util import data_validator, DataTypeError


def test_validator():
    # test the validator function
    validate_headers(["myint:int"])
    validate_headers(["myintarr:int[]"])

    try:
        validate_headers(["badint:integer"])
    except Exception as e:
        # Check correct error type and that type info is in the error message
        assert isinstance(e, TypeError)
        assert "badint" in str(e)
        assert "integer" in str(e)

    try:
        validate_headers(["badintarr:integer[]"])
    except Exception as e:
        # Check correct error type and that type info is in the error message
        assert isinstance(e, TypeError)
        assert "badintarr" in str(e)
        assert "integer[]" in str(e)


def test_data_validator():
    data_validator("int", 1)
    data_validator("long", 1)
    data_validator("float", 1.0)
    data_validator("double", 1.0)
    data_validator("boolean", "true")
    data_validator("boolean", "false")
    data_validator("byte", b"1")
    data_validator("byte", 1)
    data_validator("short", 1)
    data_validator("char", "1")
    data_validator("string", "1")
    # data_validator("point", "1")  # Not implemented yet

    try:
        data_validator("int", "1")
    except Exception as e:
        assert isinstance(e, DataTypeError)
        assert "int" in str(e)
        assert "1" in str(e)

    try:
        data_validator("notatype", "1")
    except Exception as e:
        assert isinstance(e, TypeError)
        assert "notatype" in str(e)


class MockProcessor(Processor, object):
    """A mock processor."""

    name = "mock_processor"
    node_types = ["mock_node"]
    data_type: str = NotImplemented
    col_name: str = NotImplemented
    num_nodes: int = 3

    @staticmethod
    def data_value(n: int) -> Any:
        """Return a data value for the given node."""
        raise NotImplementedError

    def get_nodes(self):
        """Return a sequence of nodes for testing."""
        for n in range(self.num_nodes):
            yield Node(
                db_ns="PUBMED",
                db_id=str(n),
                labels=["MockNode"],
                data={f"{self.col_name}:{self.data_type}": self.data_value(n)},
            )

    def get_relations(self):
        """Return a sequence of relations for testing."""
        num = self.num_nodes
        for n in range(num):
            yield Relation(
                source_ns="PUBMED",
                source_id=str(n % num),
                target_ns="PUBMED",
                target_id=str(n + 1 % num),
                rel_type="mock_relation",
                data={"an_int:int": n, f"raises_exception:{self.data_type}": n ** 2},
            )


class BadTypeProcessor(MockProcessor):
    data_type = "bad_type"
    col_name = "colname"

    @staticmethod
    def data_value(n: int) -> int:
        return n ** 2


class GoodTypeProcessor(MockProcessor):
    data_type = "int"
    col_name = "colname"

    @staticmethod
    def data_value(n: int) -> int:
        return n ** 2


class BadArrayTypeProcessor(MockProcessor):
    data_type = "bad_type[]"
    col_name = "array_colname"

    @staticmethod
    def data_value(n: int) -> str:
        m = int((n + 1) ** 2)
        return f"[{';'.join(str(k) for k in range(m))}]"


class GoodArrayTypeProcessor(MockProcessor):
    data_type = "int[]"
    col_name = "array_colname"

    @staticmethod
    def data_value(n: int) -> str:
        m = int((n + 1) ** 2)
        return f"[{';'.join(str(k) for k in range(m))}]"


def test_data_type_validator_bad():
    with TemporaryDirectory() as temp_dir:
        directory = Path(temp_dir)

        processor_dir = directory / "output"
        processor_dir.mkdir()

        # override the __init_subclass__ with the directory for this test
        MockProcessor.directory = directory
        MockProcessor.nodes_path = directory / "nodes.tsv.gz"
        MockProcessor.nodes_indra_path = directory / "nodes.pkl"
        MockProcessor.edges_path = directory / "edges.tsv.gz"

        mp = BadTypeProcessor()
        try:
            mp.dump()
        except Exception as e:
            assert isinstance(e, TypeError)
            assert "bad_type" in str(e)
        else:
            assert False, "Expected exception"


def test_data_type_validator_good():
    with TemporaryDirectory() as temp_dir:
        directory = Path(temp_dir)

        processor_dir = directory / "output"
        processor_dir.mkdir()

        # override the __init_subclass__ with the directory for this test
        GoodTypeProcessor.directory = directory
        GoodTypeProcessor.nodes_path = directory / "nodes.tsv.gz"
        GoodTypeProcessor.nodes_indra_path = directory / "nodes.pkl"
        GoodTypeProcessor.edges_path = directory / "edges.tsv.gz"

        mp = GoodTypeProcessor()
        try:
            mp.dump()
        except Exception as e:
            assert False, f"Unexpected exception: {repr(e)}"


def test_array_data_type_validator_bad():
    with TemporaryDirectory() as temp_dir:
        directory = Path(temp_dir)

        processor_dir = directory / "output"
        processor_dir.mkdir()

        # override the __init_subclass__ with the directory for this test
        BadArrayTypeProcessor.directory = directory
        BadArrayTypeProcessor.nodes_path = directory / "nodes.tsv.gz"
        BadArrayTypeProcessor.nodes_indra_path = directory / "nodes.pkl"
        BadArrayTypeProcessor.edges_path = directory / "edges.tsv.gz"

        mp = BadArrayTypeProcessor()
        try:
            mp.dump()
        except Exception as e:
            assert isinstance(e, TypeError)
            assert "bad_type[]" in str(e)
        else:
            assert False, "Expected exception"


def test_array_data_type_validator_good():
    with TemporaryDirectory() as temp_dir:
        directory = Path(temp_dir)

        processor_dir = directory / "output"
        processor_dir.mkdir()

        # override the __init_subclass__ with the directory for this test
        GoodArrayTypeProcessor.directory = directory
        GoodArrayTypeProcessor.nodes_path = directory / "nodes.tsv.gz"
        GoodArrayTypeProcessor.nodes_indra_path = directory / "nodes.pkl"
        GoodArrayTypeProcessor.edges_path = directory / "edges.tsv.gz"

        mp = GoodArrayTypeProcessor()
        try:
            mp.dump()
        except Exception as e:
            assert False, f"Unexpected exception: {repr(e)}"
