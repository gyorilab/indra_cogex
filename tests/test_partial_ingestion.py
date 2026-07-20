import csv
import gzip
import tempfile
from textwrap import dedent

from indra_cogex.sources.partial_ingestion import (
    read_file_headers,
    _row_getter,
    _set_expression,
    format_set_clauses,
    build_node_ingestion_query,
    build_relationship_ingestion_query,
    split_edge_file_by_type,
)


def test_read_file_headers():
    with tempfile.NamedTemporaryFile(suffix=".tsv.gz") as temp_file:
        with gzip.open(temp_file.name, "wt") as f:
            csv_writer = csv.writer(f, delimiter="\t")
            csv_writer.writerow(["header1", "header2", "header3"])

        headers = read_file_headers(temp_file.name)
        assert headers == ["header1", "header2", "header3"]


def test_row_getter_plain():
    get_str = _row_getter(header="prop")
    assert get_str == "row['prop']", get_str


def test_row_getter_dtype():
    get_str = _row_getter("prop:int", dtype="int")
    assert get_str == "toInteger(row['prop:int'])", get_str


def test_row_getter_str_array():
    header = "prop:string[]"
    get_str = _row_getter(header)
    assert get_str == (
        f"CASE row['{header}'] WHEN '' THEN null ELSE split(row['{header}'], ';') END"
    ), get_str


def test_row_getter_bool_array():
    # Note that 'bool' is not a Neo4j type for booleans, 'boolean' is
    header = "prop:boolean[]"
    get_str = _row_getter(header, dtype="boolean")
    assert get_str == (
        f"CASE row['{header}'] WHEN '' THEN null ELSE "
        f"[x IN split(row['{header}'], ';') | toBoolean(x)] END"
    ), get_str


def test_set_expression_plain():
    set_expr = _set_expression(header="prop")
    assert set_expr == "prop: row['prop']", set_expr


def test_set_expression_dtype():
    set_expr = _set_expression(header="prop:int")
    assert set_expr == "prop: toInteger(row['prop:int'])", set_expr


def test_set_expression_str_array():
    set_expr = _set_expression(header="prop:string[]")
    assert set_expr == (
        "prop: CASE row['prop:string[]'] WHEN '' THEN null ELSE "
        "split(row['prop:string[]'], ';') END"
    ), set_expr


def test_set_expression_float_array():
    set_expr = _set_expression(header="prop:float[]")
    assert set_expr == (
        "prop: CASE row['prop:float[]'] WHEN '' THEN null ELSE "
        "[x IN split(row['prop:float[]'], ';') | toFloat(x)] END"
    ), set_expr


def test_set_expression_escape_dot():
    set_expr = _set_expression(header="prop.name:float")
    assert set_expr == "`prop.name`: toFloat(row['prop.name:float'])", set_expr


def test_format_set_clauses():
    property_headers = [
        "prop1",
        "prop2:int",
        "prop3:string[]",
        "prop4:boolean[]",
    ]
    query_set_clause = format_set_clauses(property_headers)
    assert query_set_clause == """\
        prop1: row['prop1'],
        prop2: toInteger(row['prop2:int']),
        prop3: CASE row['prop3:string[]'] WHEN '' THEN null ELSE split(row['prop3:string[]'], ';') END,
        prop4: CASE row['prop4:boolean[]'] WHEN '' THEN null ELSE [x IN split(row['prop4:boolean[]'], ';') | toBoolean(x)] END"""


def test_format_set_clauses_null():
    query_set_clause = format_set_clauses([])
    assert query_set_clause == ""


def test_build_node_ingestion_query_import_anywhere():
    with tempfile.NamedTemporaryFile(suffix=".tsv.gz") as temp_file:
        with gzip.open(temp_file.name, "wt") as tsv_file:
            csv_writer = csv.writer(tsv_file, delimiter="\t")
            csv_writer.writerow(
                [
                    "id:ID",
                    ":LABEL",
                    "prop1",
                    "prop2:int",
                    "prop3:string[]",
                    "prop4:boolean[]",
                ]
            )

        headers = read_file_headers(temp_file.name)
        query1 = build_node_ingestion_query(
            label="TestNode",
            file_path=temp_file.name,
            headers=headers,
            import_anywhere=True,
        )
        assert query1 == dedent("""\
            LOAD CSV WITH HEADERS FROM 'file://%s' AS row
            FIELDTERMINATOR '\\t'
            CALL (row) {
                MERGE (n:TestNode {id: row['id:ID']})
                SET n += {
                    prop1: row['prop1'],
                    prop2: toInteger(row['prop2:int']),
                    prop3: CASE row['prop3:string[]'] WHEN '' THEN null ELSE split(row['prop3:string[]'], ';') END,
                    prop4: CASE row['prop4:boolean[]'] WHEN '' THEN null ELSE [x IN split(row['prop4:boolean[]'], ';') | toBoolean(x)] END
                }
            } IN TRANSACTIONS OF 10000 ROWS
            """) % temp_file.name

def test_build_node_ingestion_query_restricted_import():
    with tempfile.NamedTemporaryFile(suffix=".tsv.gz") as temp_file:
        with gzip.open(temp_file.name, "wt") as tsv_file:
            csv_writer = csv.writer(tsv_file, delimiter="\t")
            csv_writer.writerow(
                [
                    "id:ID",
                    ":LABEL",
                    "prop1",
                    "prop2:int",
                    "prop3:string[]",
                    "prop4:boolean[]",
                ]
            )

        headers = read_file_headers(temp_file.name)

        query = build_node_ingestion_query(
            label="TestNode",
            file_path=temp_file.name,
            headers=headers,
            # Assumes file is in Neo4j import folder and file path is set
            # relative to that folder
            import_anywhere=False,
        )
        assert query == dedent("""\
            LOAD CSV WITH HEADERS FROM 'file:///%s' AS row
            FIELDTERMINATOR '\\t'
            CALL (row) {
                MERGE (n:TestNode {id: row['id:ID']})
                SET n += {
                    prop1: row['prop1'],
                    prop2: toInteger(row['prop2:int']),
                    prop3: CASE row['prop3:string[]'] WHEN '' THEN null ELSE split(row['prop3:string[]'], ';') END,
                    prop4: CASE row['prop4:boolean[]'] WHEN '' THEN null ELSE [x IN split(row['prop4:boolean[]'], ';') | toBoolean(x)] END
                }
            } IN TRANSACTIONS OF 10000 ROWS
            """) % temp_file.name.split('/')[-1]


def test_build_relationship_ingestion_query_import_anywhere():
    with tempfile.NamedTemporaryFile(suffix=".tsv.gz") as temp_file:
        with gzip.open(temp_file.name, "wt") as tsv_file:
            csv_writer = csv.writer(tsv_file, delimiter="\t")
            csv_writer.writerow(
                [
                    ":START_ID",
                    ":END_ID",
                    ":TYPE",
                    "prop1",
                    "prop2:int",
                    "prop3:string[]",
                    "prop4:boolean[]",
                ]
            )

        headers = read_file_headers(temp_file.name)
        query = build_relationship_ingestion_query(
            relationship_type="rel_type",
            file_path=temp_file.name,
            start_node_label="StartNode",
            end_node_label="EndNode",
            headers=headers,
            import_anywhere=True,
            write_mode="MERGE",
            parallel_properties=None,
        )
        assert query == dedent("""\
            LOAD CSV WITH HEADERS FROM 'file://%s' AS row
            FIELDTERMINATOR '\\t'
            CALL (row) {
                MATCH (a:StartNode {id: row[':START_ID']})
                MATCH (b:EndNode {id: row[':END_ID']})
                MERGE (a)-[n:rel_type]->(b)
                SET n += {
                    prop1: row['prop1'],
                    prop2: toInteger(row['prop2:int']),
                    prop3: CASE row['prop3:string[]'] WHEN '' THEN null ELSE split(row['prop3:string[]'], ';') END,
                    prop4: CASE row['prop4:boolean[]'] WHEN '' THEN null ELSE [x IN split(row['prop4:boolean[]'], ';') | toBoolean(x)] END
                }
            } IN TRANSACTIONS OF 10000 ROWS
            """) % temp_file.name


def test_build_relationship_ingestion_query_restricted_import():
    with tempfile.NamedTemporaryFile(suffix=".tsv.gz") as temp_file:
        with gzip.open(temp_file.name, "wt") as tsv_file:
            csv_writer = csv.writer(tsv_file, delimiter="\t")
            csv_writer.writerow(
                [
                    ":START_ID",
                    ":END_ID",
                    ":TYPE",
                    "prop1",
                    "prop2:int",
                    "prop3:string[]",
                    "prop4:boolean[]",
                ]
            )

        headers = read_file_headers(temp_file.name)
        query = build_relationship_ingestion_query(
            relationship_type="rel_type",
            file_path=temp_file.name,
            start_node_label="StartNode",
            end_node_label="EndNode",
            headers=headers,
            import_anywhere=False,
            write_mode="MERGE",
            parallel_properties=None,
        )
        assert query == dedent("""\
            LOAD CSV WITH HEADERS FROM 'file:///%s' AS row
            FIELDTERMINATOR '\\t'
            CALL (row) {
                MATCH (a:StartNode {id: row[':START_ID']})
                MATCH (b:EndNode {id: row[':END_ID']})
                MERGE (a)-[n:rel_type]->(b)
                SET n += {
                    prop1: row['prop1'],
                    prop2: toInteger(row['prop2:int']),
                    prop3: CASE row['prop3:string[]'] WHEN '' THEN null ELSE split(row['prop3:string[]'], ';') END,
                    prop4: CASE row['prop4:boolean[]'] WHEN '' THEN null ELSE [x IN split(row['prop4:boolean[]'], ';') | toBoolean(x)] END
                }
            } IN TRANSACTIONS OF 10000 ROWS
            """) % temp_file.name.split("/")[-1]


def test_build_relationship_ingestion_query_parallel_edges():
    with tempfile.NamedTemporaryFile(suffix=".tsv.gz") as temp_file:
        with gzip.open(temp_file.name, "wt") as tsv_file:
            csv_writer = csv.writer(tsv_file, delimiter="\t")
            csv_writer.writerow(
                [
                    ":START_ID",
                    ":END_ID",
                    ":TYPE",
                    "prop1",
                    "prop2:int",
                    "prop3:string[]",
                    "prop4:boolean[]",
                ]
            )

        headers = read_file_headers(temp_file.name)
        query = build_relationship_ingestion_query(
            relationship_type="rel_type",
            file_path=temp_file.name,
            start_node_label="StartNode",
            end_node_label="EndNode",
            headers=headers,
            write_mode="CREATE",
            parallel_properties=None,
        )
        assert query == dedent("""\
            LOAD CSV WITH HEADERS FROM 'file:///%s' AS row
            FIELDTERMINATOR '\\t'
            CALL (row) {
                MATCH (a:StartNode {id: row[':START_ID']})
                MATCH (b:EndNode {id: row[':END_ID']})
                CREATE (a)-[n:rel_type]->(b)
                SET n += {
                    prop1: row['prop1'],
                    prop2: toInteger(row['prop2:int']),
                    prop3: CASE row['prop3:string[]'] WHEN '' THEN null ELSE split(row['prop3:string[]'], ';') END,
                    prop4: CASE row['prop4:boolean[]'] WHEN '' THEN null ELSE [x IN split(row['prop4:boolean[]'], ';') | toBoolean(x)] END
                }
            } IN TRANSACTIONS OF 10000 ROWS
            """) % temp_file.name.split("/")[-1]


def test_build_relationship_ingestion_query_parallel_props():
    with tempfile.NamedTemporaryFile(suffix=".tsv.gz") as temp_file:
        with gzip.open(temp_file.name, "wt") as tsv_file:
            csv_writer = csv.writer(tsv_file, delimiter="\t")
            csv_writer.writerow(
                [
                    ":START_ID",
                    ":END_ID",
                    ":TYPE",
                    "prop1",
                    "prop2:int",
                    "prop3:string[]",
                    "prop4:boolean[]",
                ]
            )

        headers = read_file_headers(temp_file.name)
        query = build_relationship_ingestion_query(
            relationship_type="rel_type",
            file_path=temp_file.name,
            start_node_label="StartNode",
            end_node_label="EndNode",
            headers=headers,
            write_mode="CREATE",
            parallel_properties=["prop1"],
        )
        assert query == dedent("""\
            LOAD CSV WITH HEADERS FROM 'file:///%s' AS row
            FIELDTERMINATOR '\\t'
            CALL (row) {
                MATCH (a:StartNode {id: row[':START_ID']})
                MATCH (b:EndNode {id: row[':END_ID']})
                CREATE (a)-[n:rel_type {prop1: row['prop1']}]->(b)
                SET n += {
                    prop2: toInteger(row['prop2:int']),
                    prop3: CASE row['prop3:string[]'] WHEN '' THEN null ELSE split(row['prop3:string[]'], ';') END,
                    prop4: CASE row['prop4:boolean[]'] WHEN '' THEN null ELSE [x IN split(row['prop4:boolean[]'], ';') | toBoolean(x)] END
                }
            } IN TRANSACTIONS OF 10000 ROWS
            """) % temp_file.name.split("/")[-1]


def test_split_edge_file_by_type():
    header = [
        ":START_ID",
        ":END_ID",
        ":TYPE",
        "prop1",
        "prop2:int",
        "prop3:string[]",
        "prop4:boolean[]",
    ]
    with tempfile.NamedTemporaryFile(suffix=".tsv.gz") as temp_file:
        with gzip.open(temp_file.name, "wt") as tsv_file:
            csv_writer = csv.writer(tsv_file, delimiter="\t")
            csv_writer.writerow(header)
            csv_writer.writerow(
                ["start1", "end1", "TYPE_A", "val1", "1", "a;b;c", "true;false"]
            )
            csv_writer.writerow(
                ["start2", "end2", "TYPE_B", "val2", "2", "d;e;f", "false;true"]
            )
            csv_writer.writerow(
                ["start3", "end3", "TYPE_A", "val3", "3", "g;h;i", "true;true"]
            )
        outfiles = split_edge_file_by_type(file_path=temp_file.name)
        assert len(outfiles) == 2
        assert {"TYPE_A", "TYPE_B"} == set(outfiles)

        type_a_header = read_file_headers(outfiles["TYPE_A"])
        assert type_a_header == header
        type_b_header = read_file_headers(outfiles["TYPE_B"])
        assert type_b_header == header

        with gzip.open(outfiles["TYPE_A"], "rt") as f:
            reader = csv.reader(f, delimiter="\t")
            type_a_header = next(reader)
            assert type_a_header == header
            data_rows = list(reader)
            assert len(data_rows) == 2
            assert data_rows[0] == [
                "start1",
                "end1",
                "TYPE_A",
                "val1",
                "1",
                "a;b;c",
                "true;false",
            ]
            assert data_rows[1] == [
                "start3",
                "end3",
                "TYPE_A",
                "val3",
                "3",
                "g;h;i",
                "true;true",
            ]

        with gzip.open(outfiles["TYPE_B"], "rt") as f:
            reader = csv.reader(f, delimiter="\t")
            type_b_header = next(reader)
            assert type_b_header == header
            data_rows = list(reader)
            assert len(data_rows) == 1
            assert data_rows[0] == [
                "start2",
                "end2",
                "TYPE_B",
                "val2",
                "2",
                "d;e;f",
                "false;true",
            ]


def test_node_ingestion_clinicaltrials():
    # Testing a real-world example
    with tempfile.NamedTemporaryFile(
        suffix="_ClinicalTrial.tsv.gz"
    ) as temp_file:
        with gzip.open(temp_file.name, "wt") as tsv_file:
            csv_writer = csv.writer(tsv_file, delimiter="\t")
            csv_writer.writerow(
                [
                    "id:ID",
                    ":LABEL",
                    "completion_year:int",
                    "completion_year_anticipated:boolean",
                    "last_update_year:int",
                    "phase:int",
                    "randomized:boolean",
                    "start_year:int",
                    "start_year_anticipated:boolean",
                    "status",
                    "study_type",
                    "why_stopped",
                ]
            )

        headers = read_file_headers(temp_file.name)
        query = build_node_ingestion_query(
            label="ClinicalTrial",
            file_path=temp_file.name,
            headers=headers,
            import_anywhere=False,
        )
        assert query == dedent("""\
            LOAD CSV WITH HEADERS FROM 'file:///%s' AS row
            FIELDTERMINATOR '\\t'
            CALL (row) {
                MERGE (n:ClinicalTrial {id: row['id:ID']})
                SET n += {
                    completion_year: toInteger(row['completion_year:int']),
                    completion_year_anticipated: toBoolean(row['completion_year_anticipated:boolean']),
                    last_update_year: toInteger(row['last_update_year:int']),
                    phase: toInteger(row['phase:int']),
                    randomized: toBoolean(row['randomized:boolean']),
                    start_year: toInteger(row['start_year:int']),
                    start_year_anticipated: toBoolean(row['start_year_anticipated:boolean']),
                    status: row['status'],
                    study_type: row['study_type'],
                    why_stopped: row['why_stopped']
                }
            } IN TRANSACTIONS OF 10000 ROWS
            """
            ) % temp_file.name.split("/")[-1]


def test_clinicaltrial_edges():
    # Testing a real-world example of clinicaltrial edges
    with tempfile.NamedTemporaryFile(
        suffix=".tsv.gz"
    ) as temp_file:
        with gzip.open(temp_file.name, "wt") as tsv_file:
            csv_writer = csv.writer(tsv_file, delimiter="\t")
            csv_writer.writerows(
                [
                    [
                        ":START_ID",
                        ":END_ID",
                        ":TYPE",
                        "ctgov:boolean",
                        "gilda:boolean",
                        "ref_type",
                        "source",
                    ],
                    [
                        "chebi:10023",
                        "clinicaltrials:NCT00003031",
                        "tested_in",
                        "true",
                        "true",
                        "",
                        "",
                    ],
                    [
                        "chebi:119915",
                        "clinicaltrials:NCT01488149",
                        "has_trial",
                        "false",
                        "true",
                        "",
                        "",
                    ],
                    [
                        "clinicaltrials:NCT00000489",
                        "pubmed:1512060",
                        "has_publication",
                        "",
                        "",
                        "BACKGROUND",
                        "ctgov",
                    ],
                ]
            )

        split_edge_files = split_edge_file_by_type(file_path=temp_file.name)
        assert len(split_edge_files) == 3
        assert {"tested_in", "has_trial", "has_publication"} == set(
            split_edge_files
        )
        for rel_type, file_path in split_edge_files.items():
            assert rel_type in file_path.name, \
                f"File name {file_path.name} does not contain rel_type {rel_type}"

        tested_in_file_header = read_file_headers(split_edge_files["tested_in"])
        tested_in_file_header.remove("ref_type")
        tested_in_file_header.remove("source")
        tested_in_query = build_relationship_ingestion_query(
            relationship_type="tested_in",
            file_path=split_edge_files["tested_in"],
            start_node_label="BioEntity",
            end_node_label="ClinicalTrial",
            headers=tested_in_file_header,
            import_anywhere=False,
            write_mode="MERGE",
            parallel_properties=None,
        )
        assert tested_in_query == dedent("""\
            LOAD CSV WITH HEADERS FROM 'file:///%s' AS row
            FIELDTERMINATOR '\\t'
            CALL (row) {
                MATCH (a:BioEntity {id: row[':START_ID']})
                MATCH (b:ClinicalTrial {id: row[':END_ID']})
                MERGE (a)-[n:tested_in]->(b)
                SET n += {
                    ctgov: toBoolean(row['ctgov:boolean']),
                    gilda: toBoolean(row['gilda:boolean'])
                }
            } IN TRANSACTIONS OF 10000 ROWS
            """
        ) % split_edge_files["tested_in"].name.split("/")[-1]

        has_trial_file_header = read_file_headers(split_edge_files["has_trial"])
        has_trial_file_header.remove("ref_type")
        has_trial_file_header.remove("source")
        has_trial_query = build_relationship_ingestion_query(
            relationship_type="has_trial",
            file_path=split_edge_files["has_trial"],
            start_node_label="BioEntity",
            end_node_label="ClinicalTrial",
            headers=has_trial_file_header,
            import_anywhere=False,
            write_mode="MERGE",
            parallel_properties=None,
        )
        assert has_trial_query == dedent("""\
        LOAD CSV WITH HEADERS FROM 'file:///%s' AS row
        FIELDTERMINATOR '\\t'
        CALL (row) {
            MATCH (a:BioEntity {id: row[':START_ID']})
            MATCH (b:ClinicalTrial {id: row[':END_ID']})
            MERGE (a)-[n:has_trial]->(b)
            SET n += {
                ctgov: toBoolean(row['ctgov:boolean']),
                gilda: toBoolean(row['gilda:boolean'])
            }
        } IN TRANSACTIONS OF 10000 ROWS
        """) % split_edge_files["has_trial"].name.split("/")[-1]

        has_publication_file_header = read_file_headers(split_edge_files["has_publication"])
        has_publication_file_header.remove("ctgov:boolean")
        has_publication_file_header.remove("gilda:boolean")
        has_publication_query = build_relationship_ingestion_query(
            relationship_type="has_publication",
            file_path=split_edge_files["has_publication"],
            start_node_label="ClinicalTrial",
            end_node_label="Publication",
            headers=has_publication_file_header,
            import_anywhere=False,
            write_mode="MERGE",
            parallel_properties=["source"],
        )
        assert has_publication_query == dedent("""\
        LOAD CSV WITH HEADERS FROM 'file:///%s' AS row
        FIELDTERMINATOR '\\t'
        CALL (row) {
            MATCH (a:ClinicalTrial {id: row[':START_ID']})
            MATCH (b:Publication {id: row[':END_ID']})
            MERGE (a)-[n:has_publication {source: row['source']}]->(b)
            SET n += {
                ref_type: row['ref_type']
            }
        } IN TRANSACTIONS OF 10000 ROWS
        """) % split_edge_files["has_publication"].name.split("/")[-1]
