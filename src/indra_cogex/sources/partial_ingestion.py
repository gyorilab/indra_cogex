"""An API for partial node ingestion via Cypher's LOAD CSV.

For node and edge files produced by the cogex source processors with neo4j-admin
import file header format, this module builds and runs batched MERGE or CREATE
queries from specific files. This provides an opportunity to only a subset of
nodes or relationships into an existing database.

In order to be able to ingest CSV files using Cypher queries, the following
settings need to be set for Neo4j:
Writes to the database must be enabled (see
https://neo4j.com/docs/operations-manual/current/configuration/configuration-settings/#config_server.databases.default_to_read_only,
https://neo4j.com/docs/operations-manual/current/configuration/configuration-settings/#config_server.databases.read_only, and
https://neo4j.com/docs/operations-manual/current/configuration/configuration-settings/#config_server.databases.writable)

Optionally, a third setting which allows global file loading can also be set.
This allows file loading outside the default import directory (e.g.,
/var/lib/neo4j/import/ on Linux). See more at:
https://neo4j.com/docs/operations-manual/current/configuration/configuration-settings/#config_dbms.security.allow_csv_import_from_file_urls

Read more about the LOAD CSV Cypher command at
https://neo4j.com/docs/cypher-manual/current/clauses/load-csv/


Examples:
---------

Node example:
query = \"""\
LOAD CSV WITH HEADERS FROM 'file:///nodes_ClinicalTrial.tsv.gz' AS row FIELDTERMINATOR '\t'
CALL (row) {
  MERGE (n:ClinicalTrial {id: row['id:ID']})
  SET n += {
    completion_year:             toInteger(row['completion_year:int']),
    completion_year_anticipated: toBoolean(row['completion_year_anticipated:boolean']),
    last_update_year:            toInteger(row['last_update_year:int']),
    phase:                       toInteger(row['phase:int']),
    randomized:                  toBoolean(row['randomized:boolean']),
    start_year:                  toInteger(row['start_year:int']),
    start_year_anticipated:      toBoolean(row['start_year_anticipated:boolean']),
    status:                      row['status'],
    study_type:                  row['study_type'],
    why_stopped:                 row['why_stopped']
    // Hypothetical float vector property -
    my_array:                    CASE row['my_array:float[]']
                                 WHEN '' THEN null
                                 ELSE [x IN split(row['my_array:float[]'], ';') | toFloat(x)] END
  }
} IN TRANSACTIONS OF 10000 ROWS;\"""

Edge example for tested_in relationship types:
``
LOAD CSV WITH HEADERS FROM 'file:///edges_clinicaltrials_tested_in.tsv.gz' AS row FIELDTERMINATOR '\t'
CALL (row) {
  MATCH (a:BioEntity {id: row[':START_ID']})
  MATCH (b:ClinicalTrial {id: row[':END_ID']})
  MERGE (a)-[r:tested_in]->(b)
  SET r += {
    gilda: toBoolean(row['gilda:boolean']),
    ctgov: toBoolean(row['ctgov:boolean'])
  }
} IN TRANSACTIONS OF 10000 ROWS;
``

In order to create parallel edges, the MERGE clause can be replaced with
CREATE, which will simply add another relationship. Additionally, or
alternatively, to distinguish parallel relationships by data property, that data
property is added to the relationship MERGE or CREATE clause:
``
LOAD CSV WITH HEADERS FROM 'file:///has_publication_edges.tsv.gz' AS row
FIELDTERMINATOR '\t'
CALL (row) {
  MATCH (a:ClinicalTrial {id: row[':START_ID']})
  MATCH (b:Publication {id: row[':END_ID']})
  MERGE (a)-[r:has_publication {source: row['source']}]->(b)
  SET r += {
    ref_type: CASE row['ref_type'] WHEN '' THEN null ELSE row['ref_type'] END
  }
} IN TRANSACTIONS OF 10000 ROWS;
``
"""

import csv
import gzip
import logging
from tqdm import tqdm
from pathlib import Path
from typing import Optional, Literal

from indra_cogex.client import Neo4jClient
from indra_cogex.sources.processor import validate_headers


logger = logging.getLogger(__name__)


WRITE_MODES = ["MERGE", "CREATE"]
NODE_INGESTION_QUERY = """\
LOAD CSV WITH HEADERS FROM 'file://{file_name}' AS row
FIELDTERMINATOR '\\t'
CALL (row) {{
    MERGE (n:{label} {{id: row['id:ID']}}){set_clause}
}} IN TRANSACTIONS OF {batch_size} ROWS
"""

# Optional SET clause for property assignments
SET_CLAUSE = """
    SET n += {{
{set_clauses}
    }}"""

# Relationship query template. Note that the relationship variable is set to
# 'n', this is to match the SET_CLAUSE template.
RELATIONSHIP_QUERY = """\
LOAD CSV WITH HEADERS FROM 'file://{file_name}' AS row
FIELDTERMINATOR '\\t'
CALL (row) {{
    MATCH (a:{start_label} {{id: row[':START_ID']}})
    MATCH (b:{end_label} {{id: row[':END_ID']}})
    {write_mode} (a)-[n:{rel_type}{parallel_prop}]->(b){set_clause}
}} IN TRANSACTIONS OF {batch_size} ROWS
"""


MANDATORY_NODE_COLUMNS = {"id:ID", ":LABEL"}
MANDATORY_RELATIONSHIP_COLUMNS = {":START_ID", ":END_ID", ":TYPE"}

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
    """Read the file header row from a gzipped TSV file

    Parameters
    ----------
    file_path :
        Path to the gzipped TSV file. Headers are assumed to be on the first
        line of the file.

    Returns
    -------
    :
        A list of strings containing the headers from the file in the order they
        appear in the file.
    """
    with gzip.open(file_path, "rt") as fh:
        return next(csv.reader(fh, delimiter="\t"))


def _row_getter(header: str, dtype: Optional[str] = None) -> str:
    # Helper for formatting the right hand side of a SET clause assignment. If
    # the property is an array, the string representing it needs to be split
    # into elements and possibly convert each element if it's a non-string
    # array. If it's a scalar, convert it directly.
    conversion = _SCALAR_TYPE_CONVERSIONS.get(dtype) if dtype else None

    # Array value - split on ';'
    if header.endswith("[]"):
        array_set = f"CASE row['{header}'] WHEN '' THEN null "
        array = f"split(row['{header}'], ';')"
        # Conversion needed: convert per element in array
        if conversion:
            array_set += f"ELSE [x IN {array} | {conversion}(x)] END"
        # Otherwise, use array as is
        else:
            array_set += f"ELSE {array} END"
        return array_set

    # Scalar value
    else:
        if conversion:
            return f"{conversion}(row['{header}'])"
        return f"row['{header}']"


def _set_expression(header: str) -> str:
    # Create a SET expression for a property e.g.,
    # prop_name: row["prop_name"]
    # prop_name: toInteger(row["prop_name:int"])
    # prop_name: [x IN split(row["prop_name:int[]"], ";") | toInteger(x)]
    if ":" in header:
        prop_name, dtype = header.split(":", 1)
        if ":" in dtype:
            raise ValueError(
                f"Invalid header '{header}': multiple ':' characters found"
            )
    else:
        prop_name, dtype = header, None
    if "." in prop_name:  # todo: check for other special characters that need escaping
        prop_name = f"`{prop_name}`"  # Escape property names using backticks

    # Strip array notation from dtype
    if dtype and dtype.endswith("[]"):
            dtype = dtype[:-2]

    return f"{prop_name}: {_row_getter(header, dtype=dtype)}"


def format_set_clauses(property_headers: list[str]) -> str:
    """Build the property assignments for the SET clause from TSV headers

    Parameters
    ----------
    property_headers :
        The headers from the file to read the data properties from.

    Returns
    -------
    :
        A string containing the property assignments for the SET clause, or an
        empty string if there are no properties to set. The string is indented
        by 8 spaces and each property assignment is on a new line.
    """
    lines = [_set_expression(header) for header in property_headers]
    return ",\n".join(f"        {line}" for line in lines) if lines else ""


def build_node_ingestion_query(
    label: str,
    file_path: str | Path,
    headers: Optional[list[str]] = None,
    batch_size: int = 10_000,
    import_anywhere: bool = False,
) -> str:
    """Build a batched LOAD CSV query for ingesting nodes of a single label

    Parameters
    ----------
    label :
        The Neo4j label for all nodes in the file. Note: hard coding the label
        greatly speeds up ingestion, and it is assumed the TSV file is of only
        one label. To split up the node file per label, see
        ``split_node_file_by_label``.
    file_path :
        Path to the gzipped TSV file.
    headers :
        The headers from the file to use in creating the query. If not provided,
        they will be read from the file and all headers will be used.
    batch_size :
        Number of rows per sub-transaction (default: 10,000).
    import_anywhere :
        If True, the file will be imported from the given path. If False, the
        file is assumed to be in Neo4j's import directory e.g.,
        ``/var/lib/neo4j/import/`` on Linux. Note: this setting is only used to
        adapt the file path used in the query, and does not control the Neo4j
        instance's configuration.

    Returns
    -------
    :
        A string containing the Cypher query to ingest the nodes with.
    """
    if headers is None:
        headers = read_file_headers(file_path)
    validate_headers(headers)
    if not set(headers) & MANDATORY_NODE_COLUMNS == MANDATORY_NODE_COLUMNS:
        missing_headers = MANDATORY_NODE_COLUMNS - set(headers)
        raise ValueError(
            f"Node file headers must include at least "
            f"{', '.join(MANDATORY_NODE_COLUMNS)}. {file_path} is "
            f"missing {', '.join(missing_headers)}."
        )

    property_headers = [h for h in headers if h not in MANDATORY_NODE_COLUMNS]
    set_clauses = format_set_clauses(property_headers)
    set_clause = SET_CLAUSE.format(set_clauses=set_clauses) if set_clauses else ""
    if import_anywhere:
        file_import = Path(file_path).absolute().as_posix()
    else:
        file_import = "/" + Path(file_path).name
    return NODE_INGESTION_QUERY.format(
        label=label,
        file_name=file_import,
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
        Path to the gzipped TSV file.
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
    file_name = file_path.name
    headers = read_file_headers(file_path)
    if not label:
        # Get the label from the file name: nodes_<label>.tsv.gz
        if '_' in file_name:
            label = file_name.split("_")[1].split(".")[0]
        else:
            label_col = headers.index(":LABEL")
            if label_col >= 0:
                with gzip.open(file_path, mode="rt") as f:
                    reader = csv.reader(f, delimiter="\t")
                    next(reader)  # skip header
                    first_row = next(reader)
                    label = first_row[label_col]
            else:
                raise ValueError(
                    "Could not infer label from file name or :LABEL column. "
                    "Please provide a label explicitly."
                )
    query = build_node_ingestion_query(
        label=label,
        file_path=file_path,
        headers=headers,
        batch_size=batch_size,
    )
    logger.info(f"Running ingestion query:\n{query}")
    with client.driver.session() as session:
        session.run(query)


def build_relationship_ingestion_query(
    relationship_type: str,
    file_path: str | Path,
    start_node_label: str,
    end_node_label: str,
    headers: Optional[list[str]] = None,
    batch_size: int = 10_000,
    import_anywhere: bool = False,
    write_mode: Literal["MERGE", "CREATE"] = "MERGE",
    parallel_properties: Optional[list[str]] = None,
) -> str:
    """Build a batched LOAD CSV query for ingesting relationships of a single type.

    Parameters
    ----------
    relationship_type :
        The Neo4j relationship type for all relations in the file. Note: hard
        coding the relationship type greatly speeds up ingestion, and it is
        assumed the TSV file is of only one type. To split up the relationship
        file per type, see ``split_edge_file_by_type``.
    file_path :
        Path to the gzipped TSV file.
    headers :
        The headers from the file to use in creating the query. If not provided,
        they will be read from the file and all headers will be used.
    start_node_label :
        The label of the start node for the relationships.
    end_node_label :
        The label of the end node for the relationships.
    batch_size :
        Number of rows per sub-transaction (default: 10,000).
    import_anywhere :
        If True, the file will be imported from the given path. If False, the
        file is assumed to be in Neo4j's import directory e.g.,
        ``/var/lib/neo4j/import/`` on Linux. Note: this setting is only used to
        adapt the file path used in the query, and does not control the Neo4j
        instance's configuration.
    write_mode :
        The write mode for the relationships. Can be either "MERGE" or "CREATE".
        With MERGE, if the relationship already exists, it will be updated with
        the new properties. With CREATE, a new relationship will be created
        regardless of existing relationships.
    parallel_properties :
        List of properties to use for parallel edges.

    Returns
    -------
    :
        A string containing the Cypher query to ingest the relationships with.

    Examples
    --------
    Example relationship file headers and their corresponding query (using
    default values):

    Header: `:START_ID`, `:END_ID`, `:TYPE`, and `weight:float`
    Query:
    ``LOAD CSV WITH HEADERS FROM 'file:///edges.tsv.gz' AS row
    FIELDTERMINATOR '\\t'
    CALL (row) {
        MATCH (a:StartNode {id: row[':START_ID']})
        MATCH (b:EndNode {id: row[':END_ID']})
        MERGE (a)-[n:rel_type]->(b)
        SET n += {
            weight: toFloat(row['weight:float'])
        }
    } IN TRANSACTIONS OF 10000 ROWS;``

    Header: `:START_ID`, `:END_ID`, `:TYPE`, and `embedding:float[]`
    ``LOAD CSV WITH HEADERS FROM 'file:///edges.tsv.gz' AS row
    FIELDTERMINATOR '\\t'
    CALL (row) {
        MATCH (a:StartNode {id: row[':START_ID']})
        MATCH (b:EndNode {id: row[':END_ID']})
        MERGE (a)-[n:rel_type]->(b)
        SET n += {
            embedding: CASE row['embedding:float[]'] WHEN '' THEN null ELSE [x IN split(row['embedding:float[]'], ';') | toFloat(x)] END
        }
    } IN TRANSACTIONS OF 10000 ROWS;``
    """
    if write_mode not in WRITE_MODES:
        raise ValueError(
            f"Invalid write mode: {write_mode}. Must be one of {', '.join(WRITE_MODES)}."
        )
    # Validate headers and check for the mandatory columns for relationships
    if headers is None:
        headers = read_file_headers(file_path)
    validate_headers(headers)
    if not set(headers) & MANDATORY_RELATIONSHIP_COLUMNS == MANDATORY_RELATIONSHIP_COLUMNS:
        missing_headers = MANDATORY_RELATIONSHIP_COLUMNS - set(headers)
        raise ValueError(
            f"Relationship file headers must include at least "
            f"{', '.join(MANDATORY_RELATIONSHIP_COLUMNS)}. {file_path} is "
            f"missing {', '.join(missing_headers)}."
        )
    if parallel_properties:
        if isinstance(parallel_properties, str):
            parallel_properties = [parallel_properties]
        for par_prop in parallel_properties:
            if par_prop not in headers:
                raise ValueError(
                    f"Parallel property '{par_prop}' not found in file headers. "
                    f"Available headers: {', '.join(headers)}"
                )

    # Build the query
    property_headers = [h for h in headers if h not in MANDATORY_RELATIONSHIP_COLUMNS]
    if parallel_properties:
        parallel_props = " {" + ", ".join(
            [_set_expression(par_prop) for par_prop in parallel_properties]
        ) + "}"
        property_headers = [h for h in property_headers if h not in parallel_properties]
    else:
        parallel_props = ""

    set_clauses = format_set_clauses(property_headers)
    if import_anywhere:
        file_import = Path(file_path).absolute().as_posix()
    else:
        file_import = "/" + Path(file_path).name

    query = RELATIONSHIP_QUERY.format(
        file_name=file_import,
        start_label=start_node_label,
        end_label=end_node_label,
        rel_type=relationship_type,
        parallel_prop=parallel_props,
        set_clause=SET_CLAUSE.format(set_clauses=set_clauses) if set_clauses else "",
        batch_size=batch_size,
        write_mode=write_mode,
    )
    return query


def split_edge_file_by_type(
    file_path: str | Path, output_dir: Optional[str | Path] = None
) -> dict[str, Path]:
    """Split a gzipped edge TSV file into multiple files by relationship type.

    Parameters
    ----------
    file_path :
        Path to the gzipped TSV file
    output_dir :
        Directory to write the split files to. If not provided, writes to the
         same directory as the input relationship file

    Returns
    -------
    :
        A dictionary where the key is the relationship type and the value is the
        path to the split file for that relationship type
    """
    file_path = Path(file_path)
    input_lines = 0
    with gzip.open(file_path, mode="rt") as f:
        reader = csv.reader(f, delimiter="\t")
        header = next(reader)
        type_col = header.index(":TYPE")
        if type_col < 0:
            raise ValueError(f":TYPE column missing from {file_path}")
        types = set()
        for row in reader:
            rel_type = row[type_col]
            types.add(rel_type)
            input_lines += 1

    if len(types) == 1:
        rel_type = types.pop()
        if not rel_type:
            raise ValueError(f"Relationship type column seems empty in {file_path}")
        logger.info("Relationship file is already of single type")
        return {rel_type: file_path}

    if output_dir is None:
        output_dir = file_path.parent
    else:
        output_dir = Path(output_dir)
    file_name_prefix = file_path.name.split(".")[0]

    # Split by file: output files are <file_name>_<rel_type>.tsv.gz
    file_name_map = {
        rel_type: output_dir / f"{file_name_prefix}_{rel_type}.tsv.gz"
        for rel_type in types
    }
    with gzip.open(file_path, mode="rt") as f:
        reader = csv.reader(f, delimiter="\t")
        _ = next(reader)  # Header already read above

        # Open all output files
        file_io = {}
        for rel_type, out_path in file_name_map.items():
            fh = gzip.open(out_path, "wt")
            csv_writer = csv.writer(fh, delimiter="\t")
            csv_writer.writerow(header)
            file_io[rel_type] = {"file_handle": fh, "csv_writer": csv_writer}

        # Loop file and write to respective file
        for row in tqdm(
            reader,
            total=input_lines,
            unit="row",
            unit_scale=True,
            desc="Writing split files",
        ):
            rel_type = row[type_col]
            type_writer = file_io[rel_type]["csv_writer"]
            type_writer.writerow(row)

        # Close all open files
        for out_io in file_io.values():
            out_io["file_handle"].close()

    return file_name_map


def ingest_relations_from_file_by_type(
    client: Neo4jClient,
    file_path: str | Path,
    start_node_label: str,
    end_node_label: str,
    relationship_type: Optional[str] = None,
    batch_size: int = 10_000,
    import_anywhere: bool = False,
    write_mode: Literal["MERGE", "CREATE"] = "MERGE",
    parallel_properties: Optional[list[str]] = None
):
    """Ingest relations from a neo4j-admin-format TSV file into the database.

    Parameters
    ----------
    client :
        The client for the Neo4j connection to use when ingesting the relations
    file_path :
        Path to the gzipped TSV file holding the relationships to import. The
        relationships are assumed to be of one type and start and end node
        labels are assumed to have one label, respectively. This is to be able
        to hard code the type and labels, which greatly speeds up ingestion.
    start_node_label :
        The node label of the start node for all relations in the file. Note:
        it is assumed all starting nodes in the file have the same label.
    end_node_label :
        The node label of the end node for all relations in the file. Note:
        it is assumed all ending nodes in the file have the same label.
    relationship_type :
        The Neo4j relationship type for all relations in the file. Note: hard
        coding the relationship type greatly speeds up ingestion, and it is
        assumed the TSV file is of only one type. To split up the relationship
        file per type, see ``split_edge_file_by_type``.
    batch_size :
        Number of rows per sub-transaction (default: 10,000).
    import_anywhere :
        If True, the file will be imported from the given path. If False, the
        file is assumed to be in Neo4j's import directory e.g.,
        ``/var/lib/neo4j/import/`` on Linux. Note: this setting is only used to
        adapt the file path used in the query, and does not control the Neo4j
        instance's configuration.
    write_mode :
        If "CREATE", duplicate relationships, defined as same start id, type, and
        end id (and properties in parallel_properties if provided), from the
        input file will be ingested as parallel relationships. If "MERGE",
        duplicate relationships will be overwritten by the last occurrence in
        the input file, unless parallel_properties is provided, in which case
        the relationships will be distinguished by the values of the properties
        in parallel_properties.
    parallel_properties :
        List of file headers that should be used to distinguish parallel
        relationships. If provided, a property match will be added for each of
        these properties on the relationship MERGE clause.
    """
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    headers = read_file_headers(file_path)
    # Get the relationship type from the file if not provided
    if not relationship_type:
        type_col = headers.index(":TYPE")
        if type_col >= 0:
            with gzip.open(file_path, mode="rt") as f:
                reader = csv.reader(f, delimiter="\t")
                next(reader)  # skip header
                first_row = next(reader)
                relationship_type = first_row[type_col]
        else:
            raise ValueError(
                "Could not infer relationship type from file name or :TYPE "
                "column. Please provide a relationship type explicitly."
            )
    if isinstance(parallel_properties, str):
        parallel_properties = [parallel_properties]

    query = build_relationship_ingestion_query(
        relationship_type=relationship_type,
        file_path=file_path,
        headers=headers,
        start_node_label=start_node_label,
        end_node_label=end_node_label,
        batch_size=batch_size,
        import_anywhere=import_anywhere,
        write_mode=write_mode,
        parallel_properties=parallel_properties,
    )

    logger.info(
        f"Running ingestion query for relationship type '{relationship_type}':\n{query}"
    )
    with client.driver.session() as session:
        session.run(query)
