"""Tools for INDRA curation."""

import logging
from typing import Iterable, List, Optional, Set, Tuple, Type

import pandas as pd
from indra.assemblers.indranet import IndraNetAssembler
from indra.databases.hgnc_client import get_current_hgnc_id, kinases, phosphatases, tfs
from indra.resources import load_resource_json
from indra.sources.indra_db_rest import get_curations
from indra.statements import (
    Autophosphorylation,
    DecreaseAmount,
    Dephosphorylation,
    IncreaseAmount,
    Phosphorylation,
    Statement,
    Transphosphorylation,
)
from networkx.algorithms import edge_betweenness_centrality

from .neo4j_client import Neo4jClient, autoclient
from .queries import get_stmts_for_mesh
from .subnetwork import indra_subnetwork_go
from ..representation import indra_stmts_from_relations

__all__ = [
    "get_prioritized_stmt_hashes",
    "get_curation_df",
    "get_go_curation_hashes",
    "get_mesh_curation_hashes",
    "get_curations",
    "get_ppi_hashes",
    "get_goa_hashes",
    "get_tf_statements",
    "get_kinase_statements",
    "get_phosphatase_statements",
]

logger = logging.getLogger(__name__)

# DATABASES = {"biogrid", "hprd", "signor", "phosphoelm", "signor", "biopax"}
DATABASES: Set[str] = {
    key
    for key, value in load_resource_json("source_info.json").items()
    if value["type"] == "database"
}


def _keep_by_source(source_counts) -> bool:
    return all(k not in DATABASES for k in source_counts)


def _get_text(stmt: Statement) -> Optional[str]:
    return stmt.evidence[0].text if stmt.evidence else None


def _get_curated_statement_hashes() -> Set[int]:
    stmt_jsons = get_curations()
    return {curation["pa_hash"] for curation in stmt_jsons}


def get_prioritized_stmt_hashes(stmts: Iterable[Statement]) -> List[int]:
    """Get prioritized hashes of statements to curate."""
    df = get_curation_df(stmts)
    return list(df["stmt_hash"])


def get_curation_df(stmts: Iterable[Statement]) -> pd.DataFrame:
    """Generate a curation dataframe from INDRA statements."""
    assembler = IndraNetAssembler(list(stmts))

    # Get centrality measurements that are unaffected by other filters
    centralities = edge_betweenness_centrality(assembler.make_model())

    # Make the dataframe but include an extra column for text.
    # This works since the INDRANet assembler currently goes one
    # evidence per row (i.e., a statement could have many rows)
    df = assembler.make_df(extra_columns=[("text", _get_text)])

    # Don't worry about curating statements with no text
    df = df[df.text.notna()]

    # Don't worry about curating statements that are already curated
    curated_statement_hashes = _get_curated_statement_hashes()
    curated_idx = df["stmt_hash"].isin(curated_statement_hashes)
    logger.info(f"removing {curated_idx.sum()} pre-curated statements")
    df = df[~curated_idx]

    # Don't worry about curating statements
    # that already have database evidence
    df = df[df["source_counts"].map(_keep_by_source)]

    # Look up centralities for remaining rows
    df["centralities"] = [
        centralities.get((left, right))
        for left, right in df[["agA_name", "agB_name"]].values
    ]

    # Sort by the most central edges
    df = df.sort_values("centralities", ascending=False)

    # If several evidences from the same statement, dont need
    # to have the hash multiple times
    df = df.drop_duplicates("stmt_hash")
    return df


@autoclient()
def get_go_curation_hashes(
    go_term: Tuple[str, str],
    *,
    client: Neo4jClient,
    include_indirect: bool = False,
    mediated: bool = False,
    upstream_controllers: bool = False,
    downstream_targets: bool = False,
) -> List[int]:
    """Get prioritized statement hashes to curate for a given GO term.

    Parameters
    ----------
    go_term :
        The GO term to query. Example: ``("GO", "GO:0006915")``
    client :
        The Neo4j client.
    include_indirect :
        Should ontological children of the given GO term
        be queried as well? Defaults to False.
    mediated:
        Should relations A->X->B be included for X not associated
        to the given GO term?
    upstream_controllers:
        Should relations A<-X->B be included for upstream controller
        X not associated to the given GO term?
    downstream_targets:
        Should relations A->X<-B be included for downstream target
        X not associated to the given GO term?

    Returns
    -------
    :
        A list of INDRA statement hashes prioritized for curation
    """
    stmts = indra_subnetwork_go(
        go_term=go_term,
        client=client,
        include_indirect=include_indirect,
        mediated=mediated,
        upstream_controllers=upstream_controllers,
        downstream_targets=downstream_targets,
    )
    return get_prioritized_stmt_hashes(stmts)


@autoclient()
def get_mesh_curation_hashes(
    mesh_term: Tuple[str, str],
    *,
    client: Neo4jClient,
    include_indirect: bool = False,
) -> List[int]:
    """Get prioritized statement hashes to curate for a given MeSH term.

    Parameters
    ----------
    mesh_term :
        The medical subject heading (MeSH) disease term to query. Example: ``("MESH", "D006009")``
    client :
        The Neo4j client.
    include_indirect :
        Should hierarchical children of the given MeSH term
        be queried as well? Defaults to False.

    Returns
    -------
    :
        A list of INDRA statement hashes prioritized for curation
    """
    stmts = get_stmts_for_mesh(
        mesh_term=mesh_term,
        include_child_terms=include_indirect,
        client=client,
    )
    return get_prioritized_stmt_hashes(stmts)


databases_str = ", ".join(f'"{d}"' for d in DATABASES)


@autoclient()
def get_ppi_hashes(
    *,
    client: Neo4jClient,
    limit: Optional[int] = None,
) -> List[int]:
    """Get prioritized statement hashes for uncurated gene-gene relationships.

    Parameters
    ----------
    client :
        The Neo4j client.
    limit :
        Number of statements to return

    Returns
    -------
    :
        A list of INDRA statement hashes prioritized for curation
    """
    query = f"""\
        MATCH (a:BioEntity)-[r:indra_rel]->(b:BioEntity)
        WITH 
            a, b, r, apoc.convert.fromJsonMap(r.source_counts) as sources
        WHERE 
            a.id STARTS WITH 'hgnc'
            and b.id STARTS WITH 'hgnc'
            and r.stmt_type in ["Complex"]
            // This checks that no sources are database
            and not apoc.coll.intersection(keys(sources), [{databases_str}])
        RETURN r.stmt_hash
        ORDER BY r.evidence_count DESC
        LIMIT {limit or 30}
    """
    return [stmt_hash for stmt_hash, in client.query_tx(query)]


@autoclient()
def get_goa_hashes(
    *,
    client: Neo4jClient,
    limit: Optional[int] = None,
) -> List[int]:
    """Get prioritized statement hashes for uncurated gene-GO annotations..

    Parameters
    ----------
    client :
        The Neo4j client.
    limit :
        Number of statements to return

    Returns
    -------
    :
        A list of INDRA statement hashes prioritized for curation
    """
    if limit is None:
        limit = 30
    query = f"""\
        MATCH (a:BioEntity)-[r:indra_rel]->(b:BioEntity)
        WHERE 
            NOT (a:BioEntity)-[:associated_with]->(b:BioEntity)
            and a.id STARTS WITH 'hgnc'
            and b.id STARTS WITH 'go'
            and r.evidence_count > 10
        RETURN r.stmt_hash
        ORDER BY r.evidence_count DESC
        LIMIT {limit or 30}
    """
    return [stmt_hash for stmt_hash, in client.query_tx(query)]


def _get_symbol_curies(symbols: Iterable[str]) -> List[str]:
    return sorted(
        f"hgnc:{get_current_hgnc_id(symbol)}"
        for symbol in symbols
        if get_current_hgnc_id(symbol)
    )


TF_CURIES = _get_symbol_curies(tfs)
TF_STMT_TYPES = [IncreaseAmount, DecreaseAmount]


@autoclient()
def get_tf_statements(
    *, client: Neo4jClient, limit: Optional[int] = None
) -> List[Statement]:
    """Get transcription factor increase amount / decrease amount."""
    return _help(
        sources=TF_CURIES,
        stmt_types=TF_STMT_TYPES,
        client=client,
        limit=limit,
    )


KINASE_CURIES = _get_symbol_curies(kinases)
KINASE_STMT_TYPES = [Phosphorylation, Autophosphorylation, Transphosphorylation]


@autoclient()
def get_kinase_statements(
    *, client: Neo4jClient, limit: Optional[int] = None
) -> List[Statement]:
    """Get kinase statements."""
    return _help(
        sources=KINASE_CURIES,
        stmt_types=KINASE_STMT_TYPES,
        client=client,
        limit=limit,
    )


PHOSPHATASE_CURIES = _get_symbol_curies(phosphatases)
PHOSPHATASE_STMT_TYPES = [Dephosphorylation]


@autoclient()
def get_phosphatase_statements(
    *, client: Neo4jClient, limit: Optional[int] = None
) -> List[Statement]:
    """Get phosphatase statements."""
    return _help(
        sources=PHOSPHATASE_CURIES,
        stmt_types=PHOSPHATASE_STMT_TYPES,
        client=client,
        limit=limit,
    )


def _help(
    *,
    sources: List[str],
    stmt_types: List[Type[Statement]],
    client: Neo4jClient,
    limit: Optional[int] = None,
) -> List[Statement]:
    query = f"""\
        MATCH p=(a:BioEntity)-[r:indra_rel]->(b:BioEntity)
        WITH 
            a, b, r, p, apoc.convert.fromJsonMap(r.source_counts) as sources
        WHERE
            a.id in {sources!r}
            AND r.stmt_type in {[t.__name__ for t in stmt_types]!r}
            AND b.id STARTS WITH 'hgnc'
            AND NOT apoc.coll.intersection(keys(sources), [{databases_str}])
        RETURN p
        ORDER BY r.evidence_count DESC
        LIMIT {limit or 30}
    """
    return indra_stmts_from_relations(
        client.neo4j_to_relation(res[0]) for res in client.query_tx(query)
    )
