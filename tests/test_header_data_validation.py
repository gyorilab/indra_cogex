from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from indra_cogex.sources import Processor
from indra_cogex.representation import Node, Relation
from indra_cogex.sources.processor import validate_headers
from indra_cogex.sources.processor_util import (
    data_validator,
    DataTypeError,  # If the value does not validate against the Neo4j data type
    UnknownTypeError  # If data_type is not recognized as a Neo4j data type
)


def test_header_validator():
    # test the header validator
    validate_headers(["myint:int"])
    validate_headers(["myintarr:int[]"])

    try:
        validate_headers(["badint:integer"])
        assert False, "Expected exception: integer should not be a valid type"
    except Exception as e:
        # Check correct error type and that type info is in the error message
        assert isinstance(e, TypeError), f"Unexpected exception: {type(e)} - {repr(e)}"
        assert "badint" in str(e)
        assert "integer" in str(e)

    try:
        validate_headers(["badintarr:integer[]"])
        assert False, "Expected exception"
    except Exception as e:
        # Check correct error type and that type info is in the error message
        assert isinstance(e, TypeError), f"Unexpected exception: {type(e)} - {repr(e)}"
        assert "badintarr" in str(e)
        assert "integer[]" in str(e)


def test_data_validator():
    data_validator("int", 1)
    data_validator("int", "1")
    data_validator("long", 1)
    data_validator("long", "1")
    data_validator("float", 1.0)
    data_validator("float", "1.0")
    data_validator("double", 1.0)
    data_validator("double", "1.0")
    data_validator("boolean", "true")
    data_validator("boolean", "false")
    data_validator("byte", b"1")
    data_validator("byte", 1)
    data_validator("short", 1)
    data_validator("short", "1")
    data_validator("char", "1")
    data_validator("string", "1")
    data_validator("string", 1)
    # data_validator("point", "1")  # Not implemented yet

    try:
        data_validator("int", "1x5d23f")
        assert False, "Expected exception"
    except Exception as e:
        assert isinstance(e, DataTypeError)
        assert "int" in str(e)
        assert "1" in str(e)

    try:
        data_validator("notatype", "1")
        assert False, "Expected exception"
    except Exception as e:
        assert isinstance(e, UnknownTypeError)
        assert "notatype" in str(e)


class MockProcessor(Processor, object):
    """A mock processor."""

    name = "mock_processor"
    node_types = ["mock_node"]
    data_type: str = NotImplemented
    rel_data_type: str = NotImplemented
    col_name: str = NotImplemented
    rel_col_name: str = NotImplemented
    num_nodes: int = 3

    @staticmethod
    def data_value(n: int) -> Any:
        """Return a data value for the given node."""
        raise NotImplementedError

    @staticmethod
    def rel_data_value(n: int) -> Any:
        """Return a data value for the given relation."""
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
            if self.rel_col_name is not None:
                data = {
                    f"{self.rel_col_name}:{self.rel_data_type}": self.rel_data_value(n)
                }
            else:
                data = None
            yield Relation(
                source_ns="PUBMED",
                source_id=str(n % num),
                target_ns="PUBMED",
                target_id=str(n + 1 % num),
                rel_type="mock_relation",
                data=data,
            )


class BadTypeProcessor(MockProcessor):
    data_type = "bad_type"
    col_name = "colname"
    rel_data_type = None
    rel_col_name = None

    @staticmethod
    def data_value(n: int) -> int:
        return n ** 2


class GoodTypeProcessor(MockProcessor):
    data_type = "int"
    col_name = "colname"
    rel_data_type = None
    rel_col_name = None

    @staticmethod
    def data_value(n: int) -> int:
        return n ** 2


class BadArrayTypeProcessor(MockProcessor):
    data_type = "bad_type[]"
    col_name = "array_colname"
    rel_data_type = None
    rel_col_name = None

    @staticmethod
    def data_value(n: int) -> str:
        m = int((n + 1) ** 2)
        return f"{';'.join(str(k) for k in range(m))}"


class GoodArrayTypeProcessor(MockProcessor):
    data_type = "int[]"
    col_name = "array_colname"
    rel_data_type = None
    rel_col_name = None

    @staticmethod
    def data_value(n: int) -> str:
        m = int((n + 1) ** 2)
        return f"{';'.join(str(k) for k in range(m))}"


class BadDataValueProcessor(MockProcessor):
    data_type = "boolean"
    col_name = "colname"
    rel_data_type = "int"
    rel_col_name = "rel_colname"

    @staticmethod
    def data_value(n: int):
        return True

    @staticmethod
    def rel_data_value(n: int):
        return True


class GoodDataValueProcessor(MockProcessor):
    data_type = "boolean"
    col_name = "colname"
    rel_data_type = "int"
    rel_col_name = "rel_colname"

    @staticmethod
    def data_value(n: int) -> str:
        return "true"

    @staticmethod
    def rel_data_value(n: int) -> int:
        return n


class BadArrayDataValueProcessor(MockProcessor):
    data_type = "int[]"
    col_name = "colname"
    rel_data_type = "float[]"
    rel_col_name = "rel_colname"

    @staticmethod
    def data_value(n: int) -> str:
        return ";".join(["notanint", "notanint"])

    @staticmethod
    def rel_data_value(n: int) -> str:
        return ";".join(["notaboolean", "true"])


class GoodArrayDataValueProcessor(MockProcessor):
    data_type = "int[]"
    col_name = "colname"
    rel_data_type = "float[]"
    rel_col_name = "rel_colname"

    @staticmethod
    def data_value(n: int) -> str:
        return f"{n};{n + 1}"

    @staticmethod
    def rel_data_value(n: int) -> str:
        return f"{n + 0.1};{n + 1.1}"


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
            # validate_headers is run before data_validator, so the error
            # will be a TypeError from validate_headers instead of a
            # DataTypeError from data_validator
            assert isinstance(e, TypeError)
            assert "bad_type" in str(e)
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


def test_data_value_validator_bad():
    with TemporaryDirectory() as temp_dir:
        directory = Path(temp_dir)

        processor_dir = directory / "output"
        processor_dir.mkdir()

        # override the __init_subclass__ with the directory for this test
        BadDataValueProcessor.directory = directory
        BadDataValueProcessor.nodes_path = directory / "nodes.tsv.gz"
        BadDataValueProcessor.nodes_indra_path = directory / "nodes.pkl"
        BadDataValueProcessor.edges_path = directory / "edges.tsv.gz"

        mp = BadDataValueProcessor()
        try:
            mp.dump()
        except Exception as e:
            assert isinstance(e, DataTypeError)
            assert "True" in str(e)
        else:
            assert False, "Expected exception"


def test_data_value_validator_good():
    with TemporaryDirectory() as temp_dir:
        directory = Path(temp_dir)

        processor_dir = directory / "output"
        processor_dir.mkdir()

        # override the __init_subclass__ with the directory for this test
        GoodDataValueProcessor.directory = directory
        GoodDataValueProcessor.nodes_path = directory / "nodes.tsv.gz"
        GoodDataValueProcessor.nodes_indra_path = directory / "nodes.pkl"
        GoodDataValueProcessor.edges_path = directory / "edges.tsv.gz"

        mp = GoodDataValueProcessor()
        _ = mp.dump()


def test_array_data_value_validator_bad():
    with TemporaryDirectory() as temp_dir:
        directory = Path(temp_dir)

        processor_dir = directory / "output"
        processor_dir.mkdir()

        # override the __init_subclass__ with the directory for this test
        BadArrayDataValueProcessor.directory = directory
        BadArrayDataValueProcessor.nodes_path = directory / "nodes.tsv.gz"
        BadArrayDataValueProcessor.nodes_indra_path = directory / "nodes.pkl"
        BadArrayDataValueProcessor.edges_path = directory / "edges.tsv.gz"

        mp = BadArrayDataValueProcessor()
        try:
            mp.dump()
        except Exception as e:
            assert isinstance(e, DataTypeError), f"Unexpected exception: {repr(e)}"
            assert any(
                s in str(e) for s in ["notanint", "notaboolean", "true"]
            ), (f"Excpected exception to contain 'notanint', 'notaboolean', "
                f"or 'true', but got {repr(e)}")
        else:
            assert False, "Expected exception of type DataTypeError"


def test_array_data_value_validator_good():
    with TemporaryDirectory() as temp_dir:
        directory = Path(temp_dir)

        processor_dir = directory / "output"
        processor_dir.mkdir()

        # override the __init_subclass__ with the directory for this test
        GoodArrayDataValueProcessor.directory = directory
        GoodArrayDataValueProcessor.nodes_path = directory / "nodes.tsv.gz"
        GoodArrayDataValueProcessor.nodes_indra_path = directory / "nodes.pkl"
        GoodArrayDataValueProcessor.edges_path = directory / "edges.tsv.gz"

        mp = GoodArrayDataValueProcessor()
        try:
            mp.dump()
        except Exception as e:
            assert False, f"Unexpected exception: {repr(e)}"
