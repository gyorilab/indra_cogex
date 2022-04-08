"""Tools for INDRA curation."""

import logging
from functools import lru_cache
from itertools import chain
from typing import Iterable, List, Mapping, Optional, Set, Tuple, Type

import pandas as pd
from indra.assemblers.indranet import IndraNetAssembler
from indra.databases.hgnc_client import get_current_hgnc_id, kinases, phosphatases, tfs
from indra.databases.mirbase_client import _hgnc_id_to_mirbase_id
from indra.ontology.bio import bio_ontology
from indra.sources.indra_db_rest import get_curations
from indra.statements import (
    Activation,
    Complex,
    DecreaseAmount,
    Dephosphorylation,
    Deubiquitination,
    IncreaseAmount,
    Inhibition,
    Phosphorylation,
    Statement,
)
from networkx.algorithms import edge_betweenness_centrality

from .neo4j_client import Neo4jClient, autoclient
from .subnetwork import indra_subnetwork_go
from ..constants import DATABASES
from ..representation import indra_stmts_from_relations
from ..resources import ensure_disprot

__all__ = [
    "get_prioritized_stmt_hashes",
    "get_curation_df",
    "get_go_curation_hashes",
    "get_curations",
    "get_ppi_evidence_counts",
    "get_goa_evidence_counts",
    "get_tf_statements",
    "get_kinase_statements",
    "get_phosphatase_statements",
    "get_conflicting_statements",
    "get_dub_statements",
    "get_entity_evidence_counts",
    "get_mirna_statements",
    "get_disprot_statements",
]

logger = logging.getLogger(__name__)


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


databases_str = ", ".join(f'"{d}"' for d in DATABASES)


def _limit_line(limit: Optional[int] = None) -> str:
    if limit is None:
        return ""
    if limit <= 0:
        raise ValueError("Limit must be above 0")
    return f"LIMIT {limit}"


@autoclient()
def get_ppi_evidence_counts(
    *,
    client: Neo4jClient,
    limit: Optional[int] = None,
) -> Mapping[int, int]:
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
        RETURN r.stmt_hash, r.evidence_count
        ORDER BY r.evidence_count DESC
        {_limit_line(limit)}
    """
    return client.query_dict(query)


@autoclient()
def get_goa_evidence_counts(
    *,
    client: Neo4jClient,
    limit: Optional[int] = None,
) -> Mapping[int, int]:
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
    query = f"""\
        MATCH (a:BioEntity)-[r:indra_rel]->(b:BioEntity)
        WHERE
            NOT (a:BioEntity)-[:associated_with]->(b:BioEntity)
            and a.id STARTS WITH 'hgnc'
            and b.id STARTS WITH 'go'
            and r.evidence_count > 10
        RETURN r.stmt_hash, r.evidence_count
        ORDER BY r.evidence_count DESC
        {_limit_line(limit)}
    """
    return client.query_dict(query)


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
) -> Mapping[int, int]:
    """Get transcription factor increase amount / decrease amount."""
    return _help(
        sources=TF_CURIES,
        stmt_types=TF_STMT_TYPES,
        client=client,
        limit=limit,
    )


KINASE_CURIES = _get_symbol_curies(kinases)
KINASE_STMT_TYPES = [
    Phosphorylation,
    # Autophosphorylation,
    # #Transphosphorylation,
]


@autoclient()
def get_kinase_statements(
    *, client: Neo4jClient, limit: Optional[int] = None
) -> Mapping[int, int]:
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
) -> Mapping[int, int]:
    """Get phosphatase statements."""
    return _help(
        sources=PHOSPHATASE_CURIES,
        stmt_types=PHOSPHATASE_STMT_TYPES,
        client=client,
        limit=limit,
    )


@lru_cache(maxsize=1)
def _get_dub_curies():
    return _make_curies(
        hgnc_id
        for _, hgnc_id in bio_ontology.get_children(
            "FPLX", "Deubiquitinase", ns_filter="HGNC"
        )
    )


DUB_STMT_TYPES = [Deubiquitination]


@autoclient()
def get_dub_statements(
    *, client: Neo4jClient, limit: Optional[int] = None
) -> Mapping[int, int]:
    """Get deubiquitinase statements."""
    return _help(
        sources=_get_dub_curies(),
        stmt_types=DUB_STMT_TYPES,
        client=client,
        limit=limit,
    )


def _make_curies(hgnc_ids: List[str]) -> List[str]:
    return [f"hgnc:{hgnc_id}" for hgnc_id in sorted(hgnc_ids, key=int)]


def _get_mirnas() -> List[str]:
    return _make_curies(_hgnc_id_to_mirbase_id)


MIRNA_CURIES = _get_mirnas()
MIRNA_STMT_TYPES = [IncreaseAmount, DecreaseAmount]


@autoclient()
def get_mirna_statements(
    *, client: Neo4jClient, limit: Optional[int] = None
) -> Mapping[int, int]:
    """Get miRNA statements."""
    return _help(
        sources=MIRNA_CURIES,
        stmt_types=MIRNA_STMT_TYPES,
        client=client,
        limit=limit,
    )


DISPROT_CURIES = _make_curies(ensure_disprot())
DISPROT_STMT_TYPES = {
    "hgnc": [Complex, Activation, Inhibition],
    "chebi": [Complex, IncreaseAmount, DecreaseAmount],
    "go": [Complex, Activation, Inhibition],
}


@autoclient()
def get_disprot_statements(
    *,
    client: Neo4jClient,
    limit: Optional[int] = None,
    object_prefix: Optional[str] = None,
) -> Mapping[int, int]:
    """Get statements about disordered proteins."""
    return _help(
        sources=DISPROT_CURIES,
        stmt_types=DISPROT_STMT_TYPES[object_prefix or "hgnc"],
        client=client,
        limit=limit,
        object_prefix=object_prefix,
    )


def _help(
    *,
    sources: List[str],
    stmt_types: List[Type[Statement]],
    client: Neo4jClient,
    limit: Optional[int] = None,
    object_prefix: Optional[str] = None,
) -> Mapping[int, int]:
    if object_prefix is None:
        object_prefix = "hgnc"
    query = f"""\
        MATCH p=(a:BioEntity)-[r:indra_rel]->(b:BioEntity)
        WITH
            a, b, r, p, keys(apoc.convert.fromJsonMap(r.source_counts)) as sources
        WHERE
            a.id in {sources!r}
            AND r.stmt_type in {[t.__name__ for t in stmt_types]!r}
            AND b.id STARTS WITH '{object_prefix}'
            AND NOT apoc.coll.intersection(sources, [{databases_str}])
            AND NOT sources = ['medscan']
            AND a.id <> b.id
        RETURN r.stmt_hash, r.evidence_count
        ORDER BY r.evidence_count DESC
        {_limit_line(limit)}
    """
    return client.query_dict(query)


@autoclient()
def get_entity_evidence_counts(
    prefix: str,
    identifier: str,
    *,
    client: Neo4jClient,
    limit: Optional[int] = None,
) -> Mapping[int, int]:
    query = f"""\
        MATCH p=(a:BioEntity)-[r:indra_rel]->(b:BioEntity)
        WITH
            a, b, r, p, apoc.convert.fromJsonMap(r.source_counts) as sources
        WHERE
            a.id = "{prefix}:{identifier}"
            AND NOT apoc.coll.intersection(keys(sources), [{databases_str}])
            AND a.id <> b.id
        RETURN r.stmt_hash, r.evidence_count
        ORDER BY r.evidence_count DESC
        {_limit_line(limit)}
    """
    return client.query_dict(query)


@autoclient()
def get_conflicting_statements(
    *,
    client: Neo4jClient,
    limit: Optional[int] = None,
    positive_stmt_type: Type[Statement] = Activation,
    negative_stmt_type: Type[Statement] = Inhibition,
):
    """Get statements that conflict in activation/inhibition.

    .. warning:: This takes about 10 minutes to run ATM
    """
    query = f"""\
        MATCH
            p=(a:BioEntity)-[r1:indra_rel]->(b:BioEntity)<-[r2:indra_rel]-(a:BioEntity)
        WITH
            a, b, p,
            r1, apoc.convert.fromJsonMap(r1.source_counts) as r1_sources,
            r2, apoc.convert.fromJsonMap(r1.source_counts) as r2_sources,
            r1.evidence_count + r2.evidence_count as total_evidence_count
        WHERE
            a.id STARTS WITH 'hgnc'
            AND b.id STARTS WITH 'hgnc'
            AND r1.stmt_type in ['{positive_stmt_type.__name__}']
            AND r2.stmt_type in ['{negative_stmt_type.__name__}']
            AND (
                NOT apoc.coll.intersection(keys(r1_sources), [{databases_str}])
                OR NOT apoc.coll.intersection(keys(r2_sources), [{databases_str}])
            )
        RETURN p
        ORDER BY total_evidence_count DESC
        {_limit_line(limit)}
    """
    res = client.query_tx(query)
    return indra_stmts_from_relations(
        chain.from_iterable(client.neo4j_to_relations(row[0]) for row in res)
    )
