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
from indra.util.statement_presentation import db_sources
from networkx.algorithms import edge_betweenness_centrality

from .neo4j_client import Neo4jClient, autoclient
from .subnetwork import indra_subnetwork_go
from ..apps.proxies import curation_cache
from ..representation import indra_stmts_from_relations
from ..resources import ensure_disprot

__all__ = [
    "get_prioritized_stmt_hashes",
    "get_curation_df",
    "get_go_curation_hashes",
    "get_ppi_source_counts",
    "get_goa_source_counts",
    "get_tf_statements",
    "get_kinase_statements",
    "get_phosphatase_statements",
    "get_conflicting_statements",
    "get_dub_statements",
    "get_entity_source_counts",
    "get_mirna_statements",
    "get_disprot_statements",
]

logger = logging.getLogger(__name__)


def _keep_by_source(source_counts) -> bool:
    return all(k not in db_sources for k in source_counts)


def _get_text(stmt: Statement) -> Optional[str]:
    return stmt.evidence[0].text if stmt.evidence else None


def _get_curated_statement_hashes() -> Set[int]:
    curation_list = curation_cache.get_curation_cache()
    return {curation["pa_hash"] for curation in curation_list}


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


def _limit_line(limit: Optional[int] = None) -> str:
    if limit is None:
        return ""
    if limit <= 0:
        raise ValueError("Limit must be above 0")
    return f"LIMIT {limit}"


@autoclient()
def get_ppi_source_counts(
    *,
    client: Neo4jClient,
    minimum_evidences: int = 20,
) -> Mapping[int, Mapping[str, int]]:
    """Get prioritized statement hashes for uncurated gene-gene relationships.

    Parameters
    ----------
    client :
        The Neo4j client.

    minimum_evidences :
        note that as the minimum number of evidences is lowered,
        many more statements are returned which makes the query much slower.
        for example, with no cutoff, this took 26 seconds~ for about 70K statments.
        with a minimum of 10, this took around 15 seconds. With a cutoff of
        20, this took about 10 seconds for 6.5k statements

    Returns
    -------
    :
        A list of INDRA statement hashes prioritized for curation
    """
    query = f"""\
        MATCH (a:BioEntity)-[r:indra_rel]->(b:BioEntity)
        WHERE
            a.id STARTS WITH 'hgnc'
            AND b.id STARTS WITH 'hgnc'
            AND r.stmt_type = 'Complex'
            AND a.id < b.id
            AND NOT r.has_database_evidence
            AND r.evidence_count > {minimum_evidences}
        RETURN r.stmt_hash, r.source_counts
    """
    return client.query_dict_value_json(query)


@autoclient()
def get_goa_source_counts(
    *,
    client: Neo4jClient,
    minimum_evidences: int = 10,
) -> Mapping[int, Mapping[str, int]]:
    """Get prioritized statement hashes for uncurated gene-GO annotations..

    Parameters
    ----------
    client :
        The Neo4j client.

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
            and r.evidence_count > {minimum_evidences}
        RETURN r.stmt_hash, r.source_counts
    """
    return client.query_dict_value_json(query)


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
) -> Mapping[int, Mapping[str, int]]:
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
) -> Mapping[int, Mapping[str, int]]:
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
) -> Mapping[int, Mapping[str, int]]:
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
) -> Mapping[int, Mapping[str, int]]:
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
) -> Mapping[int, Mapping[str, int]]:
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
) -> Mapping[int, Mapping[str, int]]:
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
    minimum_evidences: int = 3,
    object_prefix: Optional[str] = None,
) -> Mapping[int, Mapping[str, int]]:
    """
    Get relations that are:

    1. **not** medscan only
    2. are **not** supported by database evidence
    3. Have a minimum evidence count

    Returns
    -------
    A mapping from statement hashes to source count dictionaries
    """
    if object_prefix is None:
        object_prefix = "hgnc"
    query = f"""\
        MATCH p=(a:BioEntity)-[r:indra_rel]->(b:BioEntity)
        WHERE
            a.id in {sources!r}
            AND r.stmt_type in {[t.__name__ for t in stmt_types]!r}
            AND b.id STARTS WITH '{object_prefix}'
            AND NOT r.has_database_evidence
            AND NOT r.medscan_only
            AND a.id <> b.id
            AND r.evidence_count > {minimum_evidences}
        RETURN r.stmt_hash, r.source_counts
    """
    return client.query_dict_value_json(query)


@autoclient()
def get_entity_source_counts(
    prefix: str,
    identifier: str,
    *,
    client: Neo4jClient,
    limit: Optional[int] = None,
) -> Mapping[int, Mapping[str, int]]:
    query = f"""\
        MATCH p=(a:BioEntity)-[r:indra_rel]->(b:BioEntity)
        WHERE
            a.id = "{prefix}:{identifier}"
            AND NOT r.has_database_evidence
            AND a.id <> b.id
        RETURN r.stmt_hash, r.source_counts
        ORDER BY r.evidence_count DESC
        {_limit_line(limit)}
    """
    return client.query_dict_value_json(query)


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
            p, r1, r2, r1.evidence_count + r2.evidence_count as total_evidence_count
        WHERE
            a.id STARTS WITH 'hgnc'
            AND b.id STARTS WITH 'hgnc'
            AND r1.stmt_type in ['{positive_stmt_type.__name__}']
            AND r2.stmt_type in ['{negative_stmt_type.__name__}']
            AND (NOT r1.has_database_evidence OR NOT r2.has_database_evidence)
        RETURN p
        ORDER BY total_evidence_count DESC
        {_limit_line(limit)}
    """
    # TODO make this more efficient
    res = client.query_tx(query, squeeze=True)
    return indra_stmts_from_relations(
        chain.from_iterable(client.neo4j_to_relations(row) for row in res)
    )
