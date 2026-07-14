"""An API for partial node ingestion via Cypher LOAD CSV.

For node and edge files produced by the cogex processors with neo4j-admin import
format, this module builds and runs batched MERGE queries when only a subset of
nodes needs to be loaded into an existing database.
"""


# Node example:
# ct_query = """\
# LOAD CSV WITH HEADERS FROM 'file:///nodes_ClinicalTrial.tsv.gz' AS row FIELDTERMINATOR '\t'
# CALL (row) {
#   MERGE (n:ClinicalTrial {id: row['id:ID']})
#   SET n += {
#     completion_year:             toInteger(row['completion_year:int']),
#     completion_year_anticipated: toBoolean(row['completion_year_anticipated:boolean']),
#     last_update_year:            toInteger(row['last_update_year:int']),
#     phase:                       toInteger(row['phase:int']),
#     randomized:                  toBoolean(row['randomized:boolean']),
#     start_year:                  toInteger(row['start_year:int']),
#     start_year_anticipated:      toBoolean(row['start_year_anticipated:boolean']),
#     status:                      row['status'],
#     study_type:                  row['study_type'],
#     why_stopped:                 row['why_stopped']
#     // Hypothetical float vector property -
#     my_array:                    CASE row['my_array:float[]']
#                                  WHEN '' THEN null
#                                  ELSE [x IN split(row['my_array:float[]'], ';') | toFloat(x)] END
#     // String vector
#   }
# } IN TRANSACTIONS OF 10000 ROWS;"""
#
# Edge example:
# query_tested_in = """\
# LOAD CSV WITH HEADERS FROM 'file:///edges_clinicaltrials_tested_in.tsv.gz' AS row FIELDTERMINATOR '\t'
# CALL (row) {
#   MATCH (a:BioEntity {id: row[':START_ID']})
#   MATCH (b:ClinicalTrial {id: row[':END_ID']})
#   MERGE (a)-[r:tested_in]->(b)
#   SET r += {
#     gilda: toBoolean(row['gilda:boolean']),
#     mesh:  toBoolean(row['mesh:boolean'])
#   }
# } IN TRANSACTIONS OF 10000 ROWS;"""


import csv
import gzip
import logging
from pathlib import Path
from typing import Optional

from indra_cogex.client import Neo4jClient
from indra_cogex.sources.processor import validate_headers


logger = logging.getLogger(__name__)


NODE_INGESTION_QUERY = """\
LOAD CSV WITH HEADERS FROM 'file://{file_name}' AS row
FIELDTERMINATOR '\\t'
CALL (row) {{
  MERGE (n:{label} {{id: row['id:ID']}}){set_clause}
}} IN TRANSACTIONS OF {batch_size} ROWS
"""

# Optional SET clause for property assignments. If there are no properties to
# set, this will be empty.
SET_CLAUSE = """
  SET n += {{
{set_clauses}
  }}"""

MANDATORY_HEADERS = {"id:ID", ":LABEL"}

_SCALAR_TYPE_CONVERSIONS = {
    "int": "toInteger",
    "integer": "toInteger",
    "long": "toInteger",
    "short": "toInteger",
    "byte": "toInteger",
    "float": "toFloat",
    "double": "toFloat",
    "boolean": "toBoolean",
}


def read_file_headers(file_path: str | Path) -> list[str]:
    """Read the header row from a gzipped neo4j-admin node TSV file."""
    with gzip.open(file_path, "rt") as fh:
        return next(csv.reader(fh, delimiter="\t"))


def _row_getter(dtype: Optional[str], header: str) -> str:
    # Helper for format the right hand side of a SET clause assignment. If the
    # property is an array, we need to split the string and convert each
    # element for non-string arrays. If it's a scalar, we just convert it
    # directly.
    conversion = _SCALAR_TYPE_CONVERSIONS.get(dtype) if dtype else None

    # Array value - split on ';'
    if header.endswith("[]"):
        array_set = f"CASE row['{header}'] WHEN '' THEN null "
        array = "split(row['{header}'], ';')"
        # Conversion needed: convert per element in array
        if conversion:
            array_set += f"ELSE [x IN {array} | {conversion}(x)] END"
        # Otherwise, user array as is
        else:
            array_set += f"ELSE {array} END"
        return array_set

    # Scalar value
    else:
        if conversion:
            return f"{conversion}(row['{header}'])"
        return f"row['{header}']"


def _set_expression(header: str) -> str:
    # Create one SET expression e.g.,
    # 'prop_name: row["prop_header"]'
    # 'prop_name: toInteger(row["prop_header:int"])'
    # 'prop_name: [x IN split(row["prop_header:int[]"], ";") | toInteger(x)]'
    if ":" in header:
        prop_name, dtype = header.split(":", 1)
    else:
        prop_name, dtype = header, None
    if "." in prop_name:  # todo: check for other special characters that need escaping
        prop_name = f"`{prop_name}`"  # Escape property names using backticks

    # Strip array notation from dtype
    if dtype and dtype.endswith("[]"):
            dtype = dtype[:-2]

    return f"{prop_name}: {_row_getter(dtype, header)}"


def format_set_clauses_node(headers: list[str]) -> str:
    """Build the property assignments for the SET clause from TSV headers."""
    property_headers = [h for h in headers if h not in MANDATORY_HEADERS]
    lines = [_set_expression(header) for header in property_headers]
    return ",\n".join(f"    {line}" for line in lines) if lines else ""


def build_node_ingestion_query(
    label: str,
    file_name: str,
    headers: list[str],
    batch_size: int = 10_000,
) -> str:
    """Build a batched LOAD CSV query for ingesting nodes of a single label."""
    validate_headers(headers)
    for mandatory_header in MANDATORY_HEADERS:
        if mandatory_header not in headers:
            raise ValueError(f"Node file headers must include '{mandatory_header}'")

    set_clauses = format_set_clauses_node(headers)
    set_clause = SET_CLAUSE.format(set_clauses=set_clauses) if set_clauses else ""
    return NODE_INGESTION_QUERY.format(
        label=label,
        file_name=file_name,
        set_clause=set_clause,
        batch_size=batch_size,
    )


def ingest_nodes_from_file(
    client: Neo4jClient,
    file_path: str | Path,
    label: Optional[str] = None,
    batch_size: int = 10_000,
):
    """Ingest nodes from a neo4j-admin-format TSV file into the database.

    Parameters
    ----------
    client :
        The client to use to ingest the nodes.
    file_path :
        Path to the gzipped TSV file. Must be located in Neo4j's import
        directory (``/var/lib/neo4j/import/``).
    label :
        The Neo4j label for all nodes in the file. If omitted, inferred from
        the file name as ``nodes_<label>.tsv.gz``. Note: This assumes there is
        only one label.
    batch_size :
        Number of rows per sub-transaction (default: 10,000).
    """
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    import_dir = Path("/var/lib/neo4j/import/")
    if not file_path.is_relative_to(import_dir):
        raise ValueError(f"File must be in {import_dir}")
    file_name = file_path.name
    headers = read_file_headers(file_path)
    if not label:
        # Get the label from the file name: nodes_<label>.tsv.gz
        if '_' in file_name:
            label = file_name.split("_")[1].split(".")[0]
        else:
            type_col = headers.index(":LABEL")
            if type_col >= 0:
                with gzip.open(file_path, mode="rt") as f:
                    reader = csv.reader(f, delimiter="\t")
                    next(reader)  # skip header
                    first_row = next(reader)
                    label = first_row[type_col]
            else:
                raise ValueError(
                    "Could not infer label from file name or :LABEL column. "
                    "Please provide a label explicitly."
                )
    query = build_node_ingestion_query(
        label=label,
        file_name=file_name,
        headers=headers,
        batch_size=batch_size,
    )
    logger.info(f"Running ingestion query:\n{query}")
    with client.driver.session() as session:
        session.run(query)
