"""Tools for INDRA curation."""

from typing import Iterable, List, Optional, Set, Tuple

import pandas as pd
from indra.assemblers.indranet import IndraNetAssembler
from indra.resources import load_resource_json
from indra.statements import Statement
from networkx.algorithms import edge_betweenness_centrality

from .neo4j_client import Neo4jClient
from .subnetwork import indra_subnetwork_go

__all__ = [
    "get_prioritized_stmt_hashes",
    "get_curation_df",
    "get_go_curation_hashes",
]

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


def _get_curated_statement_hashes() -> set[int]:
    return set()  # FIXME implement!


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
    df = df[~df["stmt_hash"].isin(curated_statement_hashes)]

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
        The GO term to query
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
