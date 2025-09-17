# See https://neo4j.com/docs/operations-manual/4.4/tools/neo4j-admin/neo4j-admin-import/#import-tool-header-format-properties
# and
# https://neo4j.com/docs/api/python-driver/current/api.html#data-types
# for available data types.
from typing import Literal, Any

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


def _check_no_newlines(value: str):
    if "\n" in value or "\r" in value:
        raise NewLineInStringError(
            f"String value '{value}' contains a newline character."
        )


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
    elif data_type == "float" or data_type == "double":
        for val in value_list:
            if isinstance(val, str):
                # Try to convert to float
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
