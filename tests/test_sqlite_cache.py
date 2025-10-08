"""These tests require connection to the INDRA CoGEx graph database"""
import pytest

from indra_cogex.client.enrichment.utils import (
    SQLITE_CACHE_PATH,
    SQLITE_GENE_SET_TABLE,
    SQLITE_GENES_WITH_CONFIDENCE_TABLE,
    build_sqlite_cache,
    gene_set_table_datasets,
    genes_with_confidence_datasets,
)

TEST_SQLITE_CACHE_PATH = SQLITE_CACHE_PATH.parent / "sqlite_test_cache.db"


@pytest.fixture(scope="module")
def db_data():
    # Remove any existing test cache
    if TEST_SQLITE_CACHE_PATH.exists():
        TEST_SQLITE_CACHE_PATH.unlink()

    # Build the cache
    queried_cache = build_sqlite_cache(
        db_path=TEST_SQLITE_CACHE_PATH, force=True, limit=10, return_cache=True
    )

    yield queried_cache

    # Remove the test cache after tests
    if TEST_SQLITE_CACHE_PATH.exists():
        TEST_SQLITE_CACHE_PATH.unlink()


def test_0_cache_built(db_data):
    # Require the fixture just so the cache is built before this test runs
    assert TEST_SQLITE_CACHE_PATH.exists()


def test_1_cache_contents(db_data):
    # Require the fixture just so the cache is built before this test runs
    import sqlite3

    conn = sqlite3.connect(TEST_SQLITE_CACHE_PATH)
    cursor = conn.cursor()

    # Check that the tables exists and that each cache for each table exists
    cursor.execute(
        f"SELECT curie, name, value FROM {SQLITE_GENE_SET_TABLE} LIMIT 1;"
    )
    assert cursor.fetchone() is not None
    for dataset in gene_set_table_datasets.keys():
        cursor.execute(
            f"SELECT curie, name, value FROM {SQLITE_GENE_SET_TABLE} WHERE cache_name = ?",
            (dataset,),
        )
        assert cursor.fetchone() is not None

    cursor.execute(
        f"SELECT curie, name, inner_key, belief, ev_count FROM "
        f"{SQLITE_GENES_WITH_CONFIDENCE_TABLE} LIMIT 1;"
    )
    assert cursor.fetchone() is not None
    for dataset in genes_with_confidence_datasets.keys():
        cursor.execute(
            f"SELECT curie, name, inner_key, belief, ev_count FROM "
            f"{SQLITE_GENES_WITH_CONFIDENCE_TABLE} WHERE cache_name = ?",
            (dataset,),
        )
        assert cursor.fetchone() is not None

    conn.close()


def _check_gene_set_cache_equality(
    dataset_name: str, sqlite_data: dict, queried_cache
):
    data = queried_cache[SQLITE_GENE_SET_TABLE][dataset_name]
    assert isinstance(data, dict)
    # Check that the keys are the same
    assert set(data.keys()) == set(sqlite_data.keys())
    # Check that the values are the same (values are sets)
    for key, val in data.items():
        assert val == sqlite_data[key]


def _check_genes_with_confidence_cache(
    dataset_name: str, sqlite_data: dict, queried_cache
):
    data = queried_cache[SQLITE_GENES_WITH_CONFIDENCE_TABLE][dataset_name]
    assert isinstance(data, dict)
    # Check that the keys are the same
    assert set(data.keys()) == set(sqlite_data.keys())
    # Check that the values are the same (values are dicts with inner_key as key)
    for key, val in data.items():
        # Check that the inner keys are the same
        assert set(val.keys()) == set(sqlite_data[key].keys())
        for inner_key, inner_val in val.items():
            assert inner_val == sqlite_data[key][inner_key]


def test_go_cache(db_data):
    sqlite_go_data = gene_set_table_datasets["go"](
        use_sqlite_cache=True, sqlite_db_path=TEST_SQLITE_CACHE_PATH
    )
    _check_gene_set_cache_equality("go", sqlite_go_data, queried_cache=db_data)


def test_reactome_cache(db_data):
    sqlite_reactome_data = gene_set_table_datasets["reactome"](
        use_sqlite_cache=True, sqlite_db_path=TEST_SQLITE_CACHE_PATH
    )
    _check_gene_set_cache_equality(
        "reactome", sqlite_reactome_data, queried_cache=db_data
    )


def test_wikipathways_cache(db_data):
    sqlite_wikipathways_data = gene_set_table_datasets["wikipathways"](
        use_sqlite_cache=True, sqlite_db_path=TEST_SQLITE_CACHE_PATH
    )
    _check_gene_set_cache_equality(
        "wikipathways", sqlite_wikipathways_data, queried_cache=db_data
    )


def test_phenotype_gene_cache(db_data):
    sqlite_phenotype_gene_data = gene_set_table_datasets["phenotypes"](
        use_sqlite_cache=True, sqlite_db_path=TEST_SQLITE_CACHE_PATH
    )
    _check_gene_set_cache_equality(
        "phenotypes", sqlite_phenotype_gene_data, queried_cache=db_data
    )


def test_entity_to_targets_cache(db_data):
    sqlite_entity_to_targets_data = genes_with_confidence_datasets[
        "entity_to_targets"
    ](use_sqlite_cache=True, sqlite_db_path=TEST_SQLITE_CACHE_PATH)
    _check_genes_with_confidence_cache(
        "entity_to_targets", sqlite_entity_to_targets_data, queried_cache=db_data
    )


def test_entity_to_regulators_cache(db_data):
    sqlite_entity_to_regulators_data = genes_with_confidence_datasets[
        "entity_to_regulators"
    ](use_sqlite_cache=True, sqlite_db_path=TEST_SQLITE_CACHE_PATH)
    _check_genes_with_confidence_cache(
        "entity_to_regulators",
        sqlite_entity_to_regulators_data,
        queried_cache=db_data,
    )


def test_positive_stmts_cache(db_data):
    sqlite_positive_stmts_data = genes_with_confidence_datasets[
        "positive_statements"
    ](use_sqlite_cache=True, sqlite_db_path=TEST_SQLITE_CACHE_PATH)
    _check_genes_with_confidence_cache(
        "positive_statements", sqlite_positive_stmts_data, queried_cache=db_data
    )


def test_negative_stmts_cache(db_data):
    sqlite_negative_stmts_data = genes_with_confidence_datasets[
        "negative_statements"
    ](use_sqlite_cache=True, sqlite_db_path=TEST_SQLITE_CACHE_PATH)
    _check_genes_with_confidence_cache(
        "negative_statements", sqlite_negative_stmts_data, queried_cache=db_data
    )


def test_kinase_phosphosites_cache(db_data):
    sqlite_kinase_phosphosites_data = genes_with_confidence_datasets[
        "kinase_phosphosites"
    ](use_sqlite_cache=True, sqlite_db_path=TEST_SQLITE_CACHE_PATH)
    _check_genes_with_confidence_cache(
        "kinase_phosphosites",
        sqlite_kinase_phosphosites_data,
        queried_cache=db_data,
    )
