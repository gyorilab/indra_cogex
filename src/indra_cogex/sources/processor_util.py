# See https://neo4j.com/docs/operations-manual/4.4/tools/neo4j-admin/neo4j-admin-import/
# especially sections 4, 5 and 6
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
    """Raised when a data type is not recognized."""


def data_validator(data_type: str, value: Any):
    """Validate that the data type matches the value.

    Parameters
    ----------
    data_type :
        The data type to validate.
    value :
        The value to validate.

    Raises
    ------
    DataTypeError
        If the data type does not match the value.
    TypeError
        If the data type is not recognized.
    """
    if data_type == "int":
        if not isinstance(value, int):
            raise DataTypeError(f"Expected {value} of type {data_type} to be int")
    elif data_type == "long":
        if not isinstance(value, int):
            raise DataTypeError(f"Expected {value} of type {data_type} to be int")
    elif data_type == "float":
        if not isinstance(value, float):
            raise DataTypeError(f"Expected {value} of type {data_type} to be float")
    elif data_type == "double":
        if not isinstance(value, float):
            raise DataTypeError(f"Expected {value} of type {data_type} to be float")
    elif data_type == "boolean":
        if not isinstance(value, str) or value not in ("true", "false"):
            raise DataTypeError(
                f"Expected {value} of type {data_type} to be 'true' or 'false'"
            )
    elif data_type == "byte":
        if not isinstance(value, (bytes, int)):
            raise DataTypeError(f"Expected {value} of type {data_type} to be bytes or int")
    elif data_type == "short":
        if not isinstance(value, int):
            raise DataTypeError(f"Expected {value} of type {data_type} to be int")
    elif data_type == "char":
        if not isinstance(value, str):
            raise DataTypeError(f"Expected {value} of type {data_type} to be str")
    elif data_type == "string":
        if isinstance(value, (int, float)):
            value = str(value)
        if not isinstance(value, str):
            raise DataTypeError(
                f"Expected {value} of type {data_type} to be str, int or float"
            )
    elif data_type == "point":
        raise NotImplementedError("point data type validation is not implemented")
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
        if not isinstance(value, (str, int)):
            raise DataTypeError(
                f"Expected {value} of type {data_type} to be str or int"
            )
    elif data_type in ["ID", "LABEL", "START_ID", "END_ID", "TYPE"]:
        if not isinstance(value, (str, int)):
            raise DataTypeError(
                f"Expected {value} of type {data_type} to be str or int"
            )
    else:
        raise TypeError(f"Unknown data type {data_type}")
