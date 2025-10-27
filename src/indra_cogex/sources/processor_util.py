# See https://neo4j.com/docs/operations-manual/4.4/tools/neo4j-admin/neo4j-admin-import/#import-tool-header-format-properties
# and
# https://neo4j.com/docs/api/python-driver/current/api.html#data-types
# for available data types.
import csv
import gzip
import pickle
from typing import Literal, Any, Union

NEO4J_DATA_TYPES = (
    "int",
    "long",
    "float",
    "double",
    "boolean",
    "byte",
    "short",
    "char",
    "string",
    "point",
    "date",
    "localtime",
    "time",
    "localdatetime",
    "datetime",
    "duration",
    # Used in node files
    "ID",
    "LABEL",
    # Used in relationship files
    "START_ID",
    "END_ID",
    "TYPE",
)

DataTypes = Literal[
    "int",
    "long",
    "float",
    "double",
    "boolean",
    "byte",
    "short",
    "char",
    "string",
    "point",
    "date",
    "localtime",
    "time",
    "localdatetime",
    "datetime",
    "duration",
    "ID",
    "LABEL",
    "START_ID",
    "END_ID",
    "TYPE",
]


class DataTypeError(TypeError):
    """Raised when a data value is not of the expected type"""


class UnknownTypeError(TypeError):
    """Raised when a data type is not recognized."""


class NewLineInStringError(ValueError):
    """Raised when a string value contains a newline character."""


class InfinityValueError(ValueError):
    """Raised when a float value is infinity."""


class DuplicateNodeIDError(ValueError):
    """Raised when a duplicate node ID is found in a node file."""


class MissingNodeIDError(ValueError):
    """Raised when a non-existent node ID referenced in a relationship file."""


def _check_no_newlines(value: str):
    if "\n" in value or "\r" in value:
        raise NewLineInStringError(
            f"String value '{value}' contains a newline character."
        )


def _check_noinfinity(value: Union[float | str]):
    if isinstance(value, float) and (value == float("inf") or value == float("-inf")):
        raise InfinityValueError(
            f"Float value '{value}' is infinity, which is not allowed in Neo4j."
        )
    if isinstance(value, str):
        try:
            fval = float(value)
            if fval == float("inf") or fval == float("-inf"):
                raise InfinityValueError(
                    f"Float value '{value}' is infinity, which is not allowed in Neo4j."
                )
        except ValueError:
            pass


def data_validator(data_type: str, value: Any):
    """Validate that the data type matches the value.

    Parameters
    ----------
    data_type :
        The Neo4j data type to validate against.
    value :
        The value to validate.

    Raises
    ------
    DataTypeError
        If the value does not validate against the Neo4j data type.
    UnknownTypeError
        If data_type is not recognized as a Neo4j data type.
    """
    # None's are provided in the data dictionaries upon initial
    # node/relationship generation as a missing/null value. Once dumped,
    # the None's are converted to empty strings which is read in when nodes
    # are assembled. If we encounter a null value, there is no need to
    # validate it.
    null_data = {None, ""}
    if value in null_data:
        return

    if isinstance(value, str):
        value_list = value.split(";") if data_type.endswith("[]") else [value]
    else:
        value_list = [value]
    value_list = [val for val in value_list if val not in null_data]
    if not value_list:
        return
    data_type = data_type.rstrip("[]")
    if data_type == "int" or data_type == "long" or data_type == "short":
        for val in value_list:
            if isinstance(val, str):
                # Try to convert to int
                _check_noinfinity(val)
                try:
                    val = int(val)
                except ValueError as e:
                    raise DataTypeError(
                        f"Data value '{val}' is of the wrong type to conform "
                        f"with Neo4j type {data_type}. Expected a value of "
                        f"type int, but got value of type str with value "
                        f"'{val}' instead."
                    ) from e
            if not isinstance(val, int):
                raise DataTypeError(
                    f"Data value '{val}' is of the wrong type to conform with "
                    f"Neo4j type {data_type}. Expected a value of type int, "
                    f"but got value of type {type(val)} instead."
                )
            _check_noinfinity(val)
    elif data_type == "float" or data_type == "double":
        for val in value_list:
            if isinstance(val, str):
                # Try to convert to float
                _check_noinfinity(val)
                try:
                    val = float(val)
                except ValueError as e:
                    raise DataTypeError(
                        f"Data value '{val}' is of the wrong type to conform "
                        f"with Neo4j type {data_type}. Expected a value of "
                        f"type float, but got value of type str with value "
                        f"'{val}' instead."
                    ) from e
            if not isinstance(val, float):
                raise DataTypeError(
                    f"Data value '{val}' is of the wrong type to conform with "
                    f"Neo4j type {data_type}. Expected a value of type float, "
                    f"but got value of type {type(val)} instead."
                )
            _check_noinfinity(val)
    elif data_type == "boolean":
        for val in value_list:
            if not isinstance(val, str) or val not in ("true", "false"):
                raise DataTypeError(
                    f"Data value '{val}' is of the wrong type to conform with "
                    f"Neo4j type {data_type}. Expected a value of type str "
                    f"with literal value 'true' or 'false', but got value of "
                    f"type {type(val)} with value '{val}' instead."
                )
    elif data_type == "byte":
        for val in value_list:
            if not isinstance(val, (bytes, int)):
                raise DataTypeError(
                    f"Data value '{val}' is of the wrong type to conform with "
                    f"Neo4j type {data_type}. Expected a value of type bytes "
                    f"or int, but got value of type {type(val)} instead."
                )
    elif data_type == "char":
        for val in value_list:
            if not isinstance(val, str):
                raise DataTypeError(
                    f"Data value '{val}' is of the wrong type to conform with "
                    f"Neo4j type {data_type}. Expected a value of type str, "
                    f"but got value of type {type(val)} instead."
                )
            _check_no_newlines(val)
    elif data_type == "string":
        for val in value_list:
            # Catch string representations of numbers
            if isinstance(val, (int, float)):
                try:
                    val = str(val)
                except ValueError as e:
                    raise DataTypeError(
                        f"Data value '{val}' is of the wrong type to conform "
                        f"with Neo4j type {data_type}. Expected a value of "
                        f"type str, int or float, but got value of type "
                        f"{type(val)} instead."
                    ) from e
            if not isinstance(val, str):
                raise DataTypeError(
                    f"Data value '{val}' is of the wrong type to conform with "
                    f"Neo4j type {data_type}. Expected a value of type str, "
                    f"int or float, but got value of type {type(val)} instead."
                )
            _check_no_newlines(val)
    elif data_type == "point":
        raise NotImplementedError(
            "Neo4j point data type validation is not implemented"
        )
    # Todo: make stricter validation for dates and times:
    # https://neo4j.com/docs/cypher-manual/4.4/syntax/temporal/#cypher-temporal-instants
    elif data_type in [
        "date",
        "localtime",
        "time",
        "localdatetime",
        "datetime",
        "duration",
    ]:
        for val in value_list:
            if not isinstance(val, (str, int)):
                raise DataTypeError(
                    f"Data value '{val}' is of the wrong type to conform with "
                    f"Neo4j type {data_type}. Expected a value of type str "
                    f"or int, but got value of type {type(val)} instead."
                )
    elif data_type in ["ID", "LABEL", "START_ID", "END_ID", "TYPE"]:
        for val in value_list:
            if not isinstance(val, (str, int)):
                raise DataTypeError(
                    f"Data value '{val}' is of the wrong type to conform with "
                    f"Neo4j type {data_type}. Expected a value of type str "
                    f"or int, but got value of type {type(val)} instead."
                )
    else:
        raise UnknownTypeError(
            f"{data_type} is not recognized as a Neo4j data type."
        )


def check_duplicated_nodes(nodes_tsv_gz_file, nodes_pkl_file):
    """Placeholder for a function that would validate data"""
    # Check for duplicate node IDs in the nodes_tsv_gz_file
    gzipped_node_ids = set()
    with gzip.open(nodes_tsv_gz_file, "rt") as f:
        reader = csv.reader(f, delimiter="\t")
        header = next(reader)
        id_index = header.index("id:ID")
        for row in reader:
            id_value = row[id_index]
            if id_value in gzipped_node_ids:
                raise DuplicateNodeIDError(
                    f"Duplicate node ID found in {nodes_tsv_gz_file}: {id_value}"
                )
            gzipped_node_ids.add(id_value)

    # Check for duplicate node IDs in the nodes_pkl_file
    pickled_node_ids = set()
    with open(nodes_pkl_file, "rb") as f:
        nodes = pickle.load(f)
        for node in nodes:
            id_value = node.db_id
            if id_value in pickled_node_ids:
                raise DuplicateNodeIDError(
                    f"Duplicate node ID found in {nodes_pkl_file}: {id_value}"
                )
            pickled_node_ids.add(id_value)

    # Check the pickled node IDs against the gzipped node IDs
    if gzipped_node_ids != pickled_node_ids:
        missing_in_gzipped = pickled_node_ids - gzipped_node_ids
        missing_in_pickled = gzipped_node_ids - pickled_node_ids
        error_message = "Mismatch between node IDs in pickled and gzipped files:"
        if missing_in_gzipped:
            error_message += (
                f"\n  {len(missing_in_gzipped)} IDs missing in {nodes_tsv_gz_file}."
            )
        if missing_in_pickled:
            error_message += (
                f"\n  {len(missing_in_pickled)} IDs missing in {nodes_pkl_file}."
            )
        raise MissingNodeIDError(error_message)


def validate_edges_node_ids(edges_tsv_gz_file, node_ids: set[str]):
    """Validate that all node IDs in the edges file exist in the nodes file.
    Parameters
    ----------
    edges_tsv_gz_file :
        Path to the gzipped TSV file containing edges.
    node_ids :
        Set of valid node IDs from the nodes file that edges should reference.

    Raises
    ------
    MissingNodeIDError
        If a node ID in the edges file does not exist in the nodes file.
    """
    # Check for missing node IDs in the edges_tsv_gz_file
    with gzip.open(edges_tsv_gz_file, "rt") as f:
        reader = csv.reader(f, delimiter="\t")
        header = next(reader)
        start_id_index = header.index(":START_ID")
        end_id_index = header.index(":END_ID")
        for row in reader:
            start_id_value = row[start_id_index]
            end_id_value = row[end_id_index]
            if start_id_value not in node_ids:
                raise MissingNodeIDError(
                    f"Missing start node ID in edges file {edges_tsv_gz_file}: "
                    f"{start_id_value}"
                )
            if end_id_value not in node_ids:
                raise MissingNodeIDError(
                    f"Missing end node ID in edges file {edges_tsv_gz_file}: "
                    f"{end_id_value}"
                )
