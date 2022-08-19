from pathlib import Path
from tempfile import TemporaryDirectory
from indra_cogex.sources import Processor
from indra_cogex.representation import Node, Relation
from indra_cogex.sources.processor import validate_headers


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


class MockProcessor(Processor, object):
    """A mock processor."""

    name = "mock_processor"
    node_types = ["mock_node"]
    data_type: str = NotImplemented

    def get_nodes(self):
        """Return an empty sequence of nodes for testing."""
        for n in range(3):
            yield Node(
                db_ns="PUBMED",
                db_id=str(n),
                labels=["MockNode"],
                data={"an_int:int": n, f"raises_exception:{self.data_type}": n ** 2},
            )

    def get_relations(self):
        """Return an empty sequence of relations for testing."""
        num = 3
        for n in range(num):
            yield Relation(
                source_ns="PUBMED",
                source_id=str(n % num),
                target_ns="PUBMED",
                target_id=str(n + 1 % num),
                rel_type="mock_relation",
                data={"an_int:int": n, f"raises_exception:{self.data_type}": n ** 2},
            )


class BadProcessor(MockProcessor):
    data_type = "bad_type"


class GoodProcessor(MockProcessor):
    data_type = "int"


def test_data_type_validator():
    with TemporaryDirectory() as temp_dir:
        directory = Path(temp_dir)

        processor_dir = directory / "output"
        processor_dir.mkdir()

        # override the __init_subclass__ with the directory for this test
        MockProcessor.directory = directory
        MockProcessor.nodes_path = directory / "nodes.tsv.gz"
        MockProcessor.nodes_indra_path = directory / "nodes.pkl"
        MockProcessor.edges_path = directory / "edges.tsv.gz"

        mp = BadProcessor()
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
        GoodProcessor.directory = directory
        GoodProcessor.nodes_path = directory / "nodes.tsv.gz"
        GoodProcessor.nodes_indra_path = directory / "nodes.pkl"
        GoodProcessor.edges_path = directory / "edges.tsv.gz"

        mp = GoodProcessor()
        try:
            mp.dump()
        except Exception as e:
            assert False, f"Unexpected exception: {repr(e)}"
        else:
            assert True
