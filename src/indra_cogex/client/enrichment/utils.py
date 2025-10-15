# -*- coding: utf-8 -*-

"""Utilities for getting gene sets."""
import logging
import pandas as pd
import sqlite3
from tqdm import tqdm
from collections import defaultdict
from pathlib import Path
from textwrap import dedent
from typing import (
    DefaultDict,
    Dict,
    Iterable,
    Optional,
    Set,
    Tuple,
    List,
    Literal,
    Union,
)
from indra.databases.hgnc_client import is_kinase
from indra.databases.identifiers import get_ns_id_from_identifiers
from indra.ontology.bio import bio_ontology
from indra_cogex.util import load_stmt_json_str
from indra_cogex.apps.constants import PYOBO_RESOURCE_FILE_VERSIONS, APP_CACHE_MODULE
from indra_cogex.client.neo4j_client import Neo4jClient, autoclient
from indra_cogex.representation import norm_id

__all__ = [
    "collect_gene_sets",
    "get_go",
    "get_wikipathways",
    "get_reactome",
    "get_phenotype_gene_sets",
    "get_entity_to_targets",
    "get_entity_to_regulators",
    "get_kinase_phosphosites",
    "SQLITE_CACHE_PATH",
]

logger = logging.getLogger(__name__)

SQLITE_CACHE_PATH = APP_CACHE_MODULE.join(name="query_cache.db")
SQLITE_GENE_SET_TABLE = "gene_sets"
GeneSets = Literal[
    "go",
    "wikipathways",
    "reactome",
    "phenotypes",
]
SQLITE_GENES_WITH_CONFIDENCE_TABLE = "regulator_target_sets"
ConfidenceGeneSet = Literal[
    "entity_to_targets",
    "entity_to_regulators",
    "positive_statements",
    "negative_statements",
    "kinase_phosphosites",
]


@autoclient()
def collect_gene_sets(
    query: str,
    *,
    client: Neo4jClient,
    background_gene_ids: Optional[Iterable[str]] = None,
    include_ontology_children: bool = False,
) -> Dict[Tuple[str, str], Set[str]]:
    """Collect gene sets based on the given query.

    Parameters
    ----------
    query:
        A cypher query
    client :
        The Neo4j client.
    background_gene_ids :
        List of HGNC gene identifiers for the background gene set. If not
        given, all genes with HGNC IDs are used as the background.
    include_ontology_children :
        If True, extend the gene set associations with associations from
        child terms using the indra ontology

    Returns
    -------
    :
        A dictionary whose keys that are 2-tuples of CURIE and name of each queried
        item and whose values are sets of HGNC gene identifiers (as strings)
    """
    curie_to_hgnc_ids: DefaultDict[Tuple[str, str], Set[str]] = defaultdict(set)
    query_res = client.query_tx(query)
    if query_res is None:
        raise ValueError
    for curie, name, hgnc_curies in query_res:
        curie_to_hgnc_ids[curie, name].update(
            hgnc_curie.lower().replace("hgnc:", "")
            if hgnc_curie.lower().startswith("hgnc:")
            else hgnc_curie.lower()
            for hgnc_curie in hgnc_curies
        )
    res = dict(curie_to_hgnc_ids)

    if include_ontology_children:
        extend_by_ontology(res)

    # We now apply filtering to the background gene set if necessary
    if background_gene_ids:
        for curie_key, hgnc_ids in res.items():
            res[curie_key] = {
                hgnc_id for hgnc_id in hgnc_ids if hgnc_id in background_gene_ids
            }

    return res


def extend_by_ontology(gene_set_mapping: Dict[Tuple[str, str], Set[str]]):
    """Extend the gene set mapping by ontology."""

    # Keys are tuples of (curie, name)
    for curie, name in gene_set_mapping:

        # Upper case the curie and split it into prefix and identifier
        graph_ns, graph_id = curie.split(":", maxsplit=1)
        db_ns, db_id = get_ns_id_from_identifiers(graph_ns, graph_id)

        # Loop the ontology children and add them to the mapping
        for child_ns, child_id in bio_ontology.get_children(db_ns, db_id):
            child_name = bio_ontology.get_name(child_ns, child_id)

            gene_set_mapping[curie, name] |= gene_set_mapping.get(
                (norm_id(child_ns, child_id), child_name), set()
            )


@autoclient()
def collect_genes_with_confidence(
    query: str,
    *,
    background_gene_ids: Optional[Iterable[str]] = None,
    client: Neo4jClient,
) -> Dict[Tuple[str, str], Dict[str, Tuple[float, int]]]:
    """Collect gene sets based on the given query.

    Parameters
    ----------
    query :
        A Cypher query collecting gene sets.
    background_gene_ids :
        List of HGNC gene identifiers for the background gene set. If not
        given, all genes with HGNC IDs are used as the background.
    client :
        The Neo4j client.

    Returns
    -------
    :
        A dictionary whose keys are 2-tuples of CURIE and name of each queried
        item and whose values are dicts of HGNC gene identifiers (as strings)
        pointing to the maximum belief and evidence count associated with
        the given HGNC gene.
    """
    curie_to_hgnc_ids = defaultdict(dict)
    max_beliefs: Dict[Tuple[str, str, str], float] = {}
    max_ev_counts: Dict[Tuple[str, str, str], int] = {}
    query_res = client.query_tx(query)
    if query_res is None:
        raise RuntimeError("Query returned no results")
    for result in query_res:
        curie = result[0]
        name = result[1]
        hgnc_ids = set()
        for hgnc_curie, belief, ev_count in result[2]:
            hgnc_id = hgnc_curie.lower().replace("hgnc:", "")
            max_beliefs[(curie, name, hgnc_id)] = max(
                belief, max_beliefs.get((curie, name, hgnc_id), 0.0)
            )
            max_ev_counts[(curie, name, hgnc_id)] = max(
                ev_count, max_ev_counts.get((curie, name, hgnc_id), 0)
            )
            hgnc_ids.add(hgnc_id)
        curie_to_hgnc_ids[(curie, name)] = {
            hgnc_id: (
                max_beliefs[(curie, name, hgnc_id)],
                max_ev_counts[(curie, name, hgnc_id)],
            )
            for hgnc_id in hgnc_ids
        }
    curie_to_hgnc_ids = dict(curie_to_hgnc_ids)

    # We now apply filtering to the background gene set if necessary
    if background_gene_ids:
        for curie_key, hgnc_dict in curie_to_hgnc_ids.items():
            curie_to_hgnc_ids[curie_key] = {
                hgnc_id: v
                for hgnc_id, v in hgnc_dict.items()
                if hgnc_id in background_gene_ids
            }
    return curie_to_hgnc_ids


def collect_phosphosites_with_confidence(
    client: Neo4jClient,
    query: str,
    background_phosphosites: Optional[Set[Tuple[str, str]]] = None,
) -> Dict[Tuple[str, str], Dict[Tuple[str, str, str], Tuple[float, int]]]:
    """Collect phosphosites based on the given query.

    Parameters
    ----------
    client :
        The Neo4j client.
    query :
        A cypher query that returns rows with (kinase.id, kinase.name, substrate.id, substrate.name, r.stmt_json)
    background_phosphosites :
        Set of (gene, site) tuples to filter the results.

    Returns
    -------
    :
        A dictionary whose keys are 2-tuples of (kinase_curie, kinase_name)
        and whose values are dicts mapping (substrate id, substrate name, site)
        tuples to a tuple of (max_belief, max_evidence_count) for that
        phosphosite.
    """
    # Execute the query
    raw_results = client.query_tx(query)

    if raw_results is None:
        logger.warning("Phosphosite query returned no results")
        return {}

    # Process results to extract phosphosite information
    kinase_map = defaultdict(dict)
    max_beliefs: Dict[Tuple[str, str, str, str, str], float] = {}
    max_ev_counts: Dict[Tuple[str, str, str, str, str], int] = {}

    # Cache for fplx entities to avoid repeated lookups
    fplx_kinase_cache = {}

    for row in raw_results:
        # kinase id, kinase name,
        #   [substrate.id, substrate.name, r.belief, r.evidence_count, r.stmt_json]
        phosphosites = set()
        kinase_curie = row[0]
        kinase_name = row[1]

        # Check if entity is a kinase (HGNC) or kinase family (FPLX)
        if kinase_curie.startswith('hgnc:'):
            if not is_kinase(kinase_name):
                continue
        elif kinase_curie.startswith('fplx:'):
            # Check cache first
            if kinase_curie in fplx_kinase_cache:
                if not fplx_kinase_cache[kinase_curie]:
                    continue
            else:
                # Query for members using the isa relationship
                fplx_query = f"""
                MATCH (gene:BioEntity)-[:isa]->(family:BioEntity)
                WHERE family.id = '{kinase_curie}' AND gene.id STARTS WITH 'hgnc:'
                RETURN gene.name
                """
                member_results = client.query_tx(fplx_query)

                if not member_results or len(member_results) == 0:
                    fplx_kinase_cache[kinase_curie] = False
                    continue

                # Check if at least 50% of members are kinases
                kinase_count = 0
                total_count = len(member_results)

                for res in member_results:
                    gene_name = res[0]  # Get gene name from query result
                    if is_kinase(gene_name):
                        kinase_count += 1

                # Consider it a kinase family if the majority of members are kinases
                is_kinase_family = (kinase_count / total_count) >= 0.5
                fplx_kinase_cache[kinase_curie] = is_kinase_family

                if not is_kinase_family:
                    continue
        else:
            continue

        # Create the kinase key
        kinase_key = (kinase_curie, kinase_name)

        # Extract phosphosite details from stmt_json
        for subs_curie, subs_name, belief, ev_count, stmt_json_str in row[2]:
            stmt_data = load_stmt_json_str(stmt_json_str)

            # Collect residue and position if available
            residue = stmt_data.get('residue')
            position = stmt_data.get('position')

            if residue and position:
                substrate_id = subs_curie.lower().replace("hgnc:", "")
                phosphosite = (substrate_id, subs_name, f"{residue}{position}")
                conf_key = (kinase_curie, kinase_name, *phosphosite)

                max_beliefs[conf_key] = max(belief, max_beliefs.get(conf_key, 0.0))
                max_ev_counts[conf_key] = max(
                    ev_count, max_ev_counts.get(conf_key, 0)
                )
                phosphosites.add(phosphosite)

        kinase_map[kinase_key] = {
            phosphosite: (
                max_beliefs[(kinase_curie, kinase_name, *phosphosite)],
                max_ev_counts[(kinase_curie, kinase_name, *phosphosite)],
            )
            for phosphosite in phosphosites
        }

    res = dict(kinase_map)

    # Apply background filtering if provided
    if background_phosphosites:
        filtered_res = {}
        for key in list(res.keys()):
            filtered_set = {
                phosphosite for phosphosite in res[key]
                if phosphosite in background_phosphosites
            }

            if filtered_set:
                filtered_res[key] = filtered_set
            else:
                continue

        res = filtered_res

    return res


@autoclient(cache=True)
def get_go(
    *,
    background_gene_ids: Optional[Iterable[str]] = None,
    client: Neo4jClient,
    use_sqlite_cache: bool = True,
    limit: Optional[int] = None,
    sqlite_db_path: Union[Path, str] = SQLITE_CACHE_PATH,
) -> Dict[Tuple[str, str], Set[str]]:
    """Get GO gene sets.

    Parameters
    ----------
    client :
        The Neo4j client.
    background_gene_ids :
        List of HGNC gene identifiers for the background gene set. If not
        given, all genes with HGNC IDs are used as the background.
    use_sqlite_cache :
        If True, use the SQLite cache if it exists. Default: True.
    sqlite_db_path :
        Path to the SQLite database to use for caching. Default:
        APP_CACHE_MODULE found in `indra_cogex.apps.constants`.
    limit :
        If given, limit the number of GO terms returned to this number.

    Returns
    -------
    :
        A dictionary whose keys are 2-tuples of CURIE and name of each GO term
        and whose values are sets of HGNC gene identifiers (as strings)
    """
    if sqlite_db_path.exists() and use_sqlite_cache:
        gene_sets = get_sqlite_gene_set_cache(
            "go",
            background_gene_ids,
            sqlite_db_path=sqlite_db_path,
            limit=limit,
        )
    else:
        query = dedent(
            """\
            MATCH (gene:BioEntity)-[:associated_with]->(term:BioEntity)
            WHERE NOT gene.obsolete
            RETURN term.id, term.name, collect(gene.id) as gene_curies
        """
        )
        if limit is not None:
            query += f"\nLIMIT {limit}"
        gene_sets = collect_gene_sets(
            client=client,
            query=query,
            background_gene_ids=background_gene_ids,
            include_ontology_children=True,
        )
    return gene_sets


def get_sqlite_gene_set_cache(
    cache_name: GeneSets,
    background_gene_ids: Optional[Iterable[str]] = None,
    sqlite_db_path: Union[Path, str] = SQLITE_CACHE_PATH,
    limit: Optional[int] = None,
) -> Dict[Tuple[str, str], Set[str]]:
    # Connect to the SQLite database
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    # Get the subset of the table corresponding to the cache_name (first column)
    query = f"SELECT curie, name, value FROM {SQLITE_GENE_SET_TABLE} WHERE cache_name = ?"
    if limit is not None:
        query += f" LIMIT {limit}"
    cursor.execute(query, (cache_name,))
    rows = cursor.fetchall()
    conn.close()

    if len(rows) == 0:
        raise ValueError(f"No entries found in cache for {cache_name}")

    gene_sets = {}
    # Loop through the rows and build the dictionary, applying background filtering
    # if necessary
    for curie, name, gene_id in rows:
        if background_gene_ids and gene_id not in background_gene_ids:
            continue
        gene_sets.setdefault((curie, name), set()).add(gene_id)
    return gene_sets


def get_sqlite_genes_with_confidence_cache(
    cache_name: ConfidenceGeneSet,
    background_gene_ids: Optional[Iterable[str]] = None,
    sqlite_db_path: Union[Path, str] = SQLITE_CACHE_PATH,
    limit: Optional[int] = None,
) -> Dict[Tuple[str, str], Dict[str, Tuple[float, int]]]:
    """Get gene sets with confidence from the SQLite cache.

    Parameters
    ----------
    cache_name :
        The name of the cache to retrieve.
    background_gene_ids :
        List of HGNC gene identifiers for the background gene set to filter the
        returned values on. If not given, no filtering is applied.
    sqlite_db_path :
        Path to the SQLite database to use for caching. Default:
        APP_CACHE_MODULE found in `indra_cogex.apps.constants`.
    limit :
        If given, limit the number of entries returned to this number.

    Returns
    -------
    :
        A dictionary whose keys are 2-tuples of CURIE and name of each queried
        item and whose values are dicts of HGNC gene identifiers (as strings)
        pointing to the maximum belief and evidence count associated with
        the given HGNC gene.
    """
    # Connect to the SQLite database
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    # Get the subset of the table corresponding to the cache_name (first column)
    query = (f"SELECT curie, name, inner_key, belief, ev_count FROM "
             f"{SQLITE_GENES_WITH_CONFIDENCE_TABLE} WHERE cache_name = ?")
    if limit is not None:
        query += f" LIMIT {limit}"
    cursor.execute(query, (cache_name,))
    rows = cursor.fetchall()
    conn.close()
    gene_sets = {}
    if len(rows) == 0:
        raise ValueError(f"No entries found in cache for {cache_name}")
    # Loop through the rows and build the dictionary, applying background filtering
    # if necessary
    for curie, name, gene_id, belief, evidence_count in rows:
        # For kinase_phosphosites, we need to split on '|'
        if '|' in gene_id:
            check_id = gene_id.split('|')[0]
            inner_key = tuple(gene_id.split('|'))
        else:
            check_id = gene_id
            inner_key = gene_id
        if background_gene_ids and check_id not in background_gene_ids:
            continue
        gene_sets.setdefault((curie, name), {})[inner_key] = (belief, evidence_count)
    return gene_sets


@autoclient()
def get_wikipathways(
    *,
    background_gene_ids: Optional[Iterable[str]] = None,
    client: Neo4jClient,
    use_sqlite_cache: bool = True,
    limit: Optional[int] = None,
    sqlite_db_path: Union[Path, str] = SQLITE_CACHE_PATH,
) -> Dict[Tuple[str, str], Set[str]]:
    """Get WikiPathways gene sets.

    Parameters
    ----------
    client :
        The Neo4j client.
    background_gene_ids :
        List of HGNC gene identifiers for the background gene set. If not
        given, all genes with HGNC IDs are used as the background.
    use_sqlite_cache :
        If True, use the SQLite cache if it exists. Default: True.
    sqlite_db_path :
        Path to the SQLite database to use for caching. Default:
        APP_CACHE_MODULE found in `indra_cogex.apps.constants`.
    limit :
        If given, limit the number of WikiPathways returned to this number.

    Returns
    -------
    :
        A dictionary whose keys are 2-tuples of CURIE and name of each WikiPathway
        pathway and whose values are sets of HGNC gene identifiers (as strings)
    """
    if sqlite_db_path.exists() and use_sqlite_cache:
        gene_sets = get_sqlite_gene_set_cache(
            "wikipathways",
            background_gene_ids,
            sqlite_db_path=sqlite_db_path,
            limit=limit
        )
    else:
        query = dedent(
            """\
            MATCH (pathway:BioEntity)-[:haspart]->(gene:BioEntity)
            WHERE pathway.id STARTS WITH "wikipathways" and gene.id STARTS WITH "hgnc"
            AND NOT gene.obsolete
            RETURN pathway.id, pathway.name, collect(gene.id)
        """
        )
        if limit is not None:
            query += f"\nLIMIT {limit}"
        gene_sets = collect_gene_sets(
            client=client,
            query=query,
            background_gene_ids=background_gene_ids,
        )
    return gene_sets


@autoclient()
def get_reactome(
    *,
    background_gene_ids: Optional[Iterable[str]] = None,
    client: Neo4jClient,
    use_sqlite_cache: bool = True,
    limit: Optional[int] = None,
    sqlite_db_path: Union[Path, str] = SQLITE_CACHE_PATH,
) -> Dict[Tuple[str, str], Set[str]]:
    """Get Reactome gene sets.

    Parameters
    ----------
    client :
        The Neo4j client.
    background_gene_ids :
        List of HGNC gene identifiers for the background gene set. If not
        given, all genes with HGNC IDs are used as the background.
    use_sqlite_cache :
        If True, use the SQLite cache if it exists. Default: True.
    sqlite_db_path :
        Path to the SQLite database to use for caching. Default:
        APP_CACHE_MODULE found in `indra_cogex.apps.constants`.
    limit :
        If given, limit the number of Reactome pathways returned to this number.

    Returns
    -------
    :
        A dictionary whose keys are 2-tuples of CURIE and name of each Reactome
        pathway and whose values are sets of HGNC gene identifiers (as strings)
    """
    if sqlite_db_path.exists() and use_sqlite_cache:
        gene_sets = get_sqlite_gene_set_cache(
            "reactome",
            background_gene_ids,
            sqlite_db_path=sqlite_db_path,
            limit=limit
        )
    else:
        query = dedent(
            """\
            MATCH (pathway:BioEntity)-[:haspart]-(gene:BioEntity)
            WHERE pathway.id STARTS WITH "reactome" and gene.id STARTS WITH "hgnc"
            AND NOT gene.obsolete
            RETURN pathway.id, pathway.name, collect(gene.id)
        """
        )
        if limit is not None:
            query += f"\nLIMIT {limit}"
        gene_sets = collect_gene_sets(
            client=client,
            query=query,
            background_gene_ids=background_gene_ids,
        )
    return gene_sets


@autoclient(cache=True)
def get_kinase_phosphosites(
    *,
    client: Neo4jClient,
    background_phosphosites: Optional[Set[Tuple[str, str]]] = None,
    minimum_evidence_count: Optional[int] = 1,
    minimum_belief: Optional[float] = 0.0,
) -> Dict[Tuple[str, str], Set[Tuple[str, str]]]:
    """Get kinase phosphosites with confidence filtering.

    Parameters
    ----------
    client :
        The Neo4j client.
    background_phosphosites :
        Optional set of (gene, site) tuples to filter returned phosphosites.
    minimum_evidence_count :
        Minimum evidence count to include a phosphosite.
    minimum_belief :
        Minimum belief to include a phosphosite.

    Returns
    -------
    :
        A mapping from (kinase_curie, kinase_name) to a set of
        (substrate name, site) tuples representing phosphosites.
    """
    phosphosites_with_confidence = get_kinase_phosphosites_raw(
        client=client,
        background_phosphosites=background_phosphosites,
    )

    return filter_phosphosite_set_confidences(
        phosphosites_with_confidence,
        minimum_belief=minimum_belief,
        minimum_evidence_count=minimum_evidence_count,
    )


@autoclient(cache=True)
def get_kinase_phosphosites_raw(
    *,
    client: Neo4jClient,
    background_phosphosites: Optional[Set[Tuple[str, str]]] = None,
    use_sqlite_cache: bool = True,
    limit: Optional[int] = None,
    sqlite_db_path: Union[Path, str] = SQLITE_CACHE_PATH,
) -> Dict[Tuple[str, str], Dict[Tuple[str, str, str], Tuple[float, int]]]:
    """Get a mapping from each kinase to the set of phosphosites it phosphorylates

    Parameters
    ----------
    client :
        The Neo4j client.
    background_phosphosites :
        Optional set of (gene, site) tuples to filter returned phosphosites.
        Each tuple is (substrate_gene_name, site) where `site` is
        residue+position (e.g. "S123").
    use_sqlite_cache :
        If True, use the SQLite cache if it exists. Default: True.
    limit :
        If given, limit the number of kinases returned to this number.
    sqlite_db_path :
        Path to the SQLite database to use for caching. Default:
        APP_CACHE_MODULE found in `indra_cogex.apps.constants`.

    Returns
    -------
    :
        Mapping from (kinase_curie, kinase_name) to a dict mapping
        (substrate id, substrate name, site) tuples to (max_belief, max_evidence_count)
        tuples.
    """
    if sqlite_db_path.exists() and use_sqlite_cache:
        res = get_sqlite_genes_with_confidence_cache(
            "kinase_phosphosites",
            background_gene_ids=background_phosphosites,
            sqlite_db_path=sqlite_db_path,
            limit=limit
        )
    else:
        query = dedent(
            f"""\
            MATCH (kinase:BioEntity)-[r:indra_rel]->(substrate:BioEntity)
            WHERE
                r.stmt_type = 'Phosphorylation'
                AND substrate.id STARTS WITH "hgnc"
                AND NOT substrate.obsolete
            RETURN
                kinase.id,
                kinase.name,
                collect(
                    [substrate.id, substrate.name, r.belief, r.evidence_count, r.stmt_json]
                )
            """
        )
        if limit is not None:
            query += f"\nLIMIT {limit}"
        res = collect_phosphosites_with_confidence(
            client=client,
            query=query,
            background_phosphosites=background_phosphosites,
        )

    return res


@autoclient()
def get_phenotype_gene_sets(
    *,
    background_gene_ids: Optional[Iterable[str]] = None,
    client: Neo4jClient,
    use_sqlite_cache: bool = True,
    limit: Optional[int] = None,
    sqlite_db_path: Union[Path, str] = SQLITE_CACHE_PATH,
) -> Dict[Tuple[str, str], Set[str]]:
    """Get HPO phenotype gene sets.

    Parameters
    ----------
    client :
        The Neo4j client.
    background_gene_ids :
        List of HGNC gene identifiers for the background gene set. If not
        given, all genes with HGNC IDs are used as the background.
    use_sqlite_cache :
        If True, use the SQLite cache if it exists. Default: True.
    sqlite_db_path :
        Path to the SQLite database to use for caching. Default:
        APP_CACHE_MODULE found in `indra_cogex.apps.constants`.
    limit :
        If given, limit the number of phenotypes returned to this number.

    Returns
    -------
    :
        A dictionary whose keys are 2-tuples of CURIE and name of each phenotype
        gene set and whose values are sets of HGNC gene identifiers (as strings)
    """
    if sqlite_db_path.exists() and use_sqlite_cache:
        gene_sets = get_sqlite_gene_set_cache(
            "phenotypes",
            background_gene_ids,
            sqlite_db_path=sqlite_db_path,
            limit=limit
        )
    else:
        query = dedent(
            """\
            MATCH (s:BioEntity)-[:phenotype_has_gene]-(gene:BioEntity)
            WHERE s.id STARTS WITH "hp" and gene.id STARTS WITH "hgnc"
            AND NOT gene.obsolete
            RETURN s.id, s.name, collect(gene.id)
        """
        )
        if limit is not None:
            query += f"\nLIMIT {limit}"
        gene_sets = collect_gene_sets(
            client=client,
            query=query,
            background_gene_ids=background_gene_ids,
        )
    return gene_sets


def filter_gene_set_confidences(
    data: Dict[Tuple[str, str], Dict[str, Tuple[float, int]]],
    minimum_belief: Optional[float] = None,
    minimum_evidence_count: Optional[int] = None,
) -> Dict[Tuple[str, str], Set[str]]:
    """Filter the confidences from a dictionary

    Parameters
    ----------
    data :
        A dictionary mapping keys are 2-tuples of CURIE and name to dictionaries
        mapping keys of IDs to (belief, evidence_count) tuples.
    minimum_belief :
        Minimum belief to include a gene in the set. If None, no filtering
        is applied.
    minimum_evidence_count :
        Minimum evidence count to include a gene in the set. If None, no
        filtering is applied.

    Returns
    -------
    :
        A dictionary mapping keys are 2-tuples of CURIE and name to sets
        of IDs that pass the filtering criteria.
    """
    if minimum_belief is None:
        minimum_belief = 0.0
    if minimum_evidence_count is None:
        minimum_evidence_count = 0
    rv = {}
    for key, confidences in data.items():
        rv[key] = {
            identifier
            for identifier, (belief, ev_count) in confidences.items()
            if belief >= minimum_belief and ev_count >= minimum_evidence_count
        }
    return rv


def filter_phosphosite_set_confidences(
    data: Dict[Tuple[str, str], Dict[Tuple[str, str, str], Tuple[float, int]]],
    minimum_belief: Optional[float] = 0.0,
    minimum_evidence_count: Optional[int] = 0,
) -> Dict[Tuple[str, str], Set[Tuple[str, str]]]:
    """Filter the phosphosite confidences from a dictionary

    Parameters
    ----------
    data :
        A dictionary mapping keys are 2-tuples of (kinase_curie, kinase_name)
        to dictionaries mapping keys of (substrate id, substrate name, site)
        tuples to (belief, evidence_count) tuples.
    minimum_belief :
        Minimum belief to include a phosphosite in the set. If None, no filtering
        is applied.
    minimum_evidence_count :
        Minimum evidence count to include a phosphosite in the set. If None, no
        filtering is applied.

    Returns
    -------
    :
        A dictionary mapping keys are 2-tuples of (kinase_curie, kinase_name)
        to sets of (substrate id, substrate name, site) tuples that pass the
        filtering criteria.
    """
    if minimum_belief is None:
        minimum_belief = 0.0
    if minimum_evidence_count is None:
        minimum_evidence_count = 0

    filtered_data = {}
    for kinase_key, phosphosite_dict in data.items():
        res_set = set()
        for phosphosite, (belief, ev_count) in phosphosite_dict.items():
            phosphosite_id, phosphosite_name, site = phosphosite
            if belief >= minimum_belief and ev_count >= minimum_evidence_count:
                res_set.add((phosphosite_name, site))
        if res_set:
            filtered_data[kinase_key] = res_set
    return filtered_data


@autoclient()
def get_entity_to_targets(
    *,
    client: Neo4jClient,
    background_gene_ids: Optional[Iterable[str]] = None,
    minimum_evidence_count: Optional[int] = 1,
    minimum_belief: Optional[float] = 0.0,
) -> Dict[Tuple[str, str], Set[str]]:
    """Get a mapping from each entity in the INDRA database to the set of
    human genes that it regulates.

    Parameters
    ----------
    client :
        The Neo4j client.
    background_gene_ids :
        List of HGNC gene identifiers for the background gene set. If not
        given, all genes with HGNC IDs are used as the background.
    minimum_evidence_count :
        The minimum number of evidences for a relationship to count it as a regulator.
        Defaults to 1 (i.e., cutoff not applied.
    minimum_belief :
        The minimum belief for a relationship to count it as a regulator.
        Defaults to 0.0 (i.e., cutoff not applied).

    Returns
    -------
    :
        A dictionary whose keys are 2-tuples of CURIE and name of each entity
        and whose values are sets of HGNC gene identifiers (as strings)
    """
    genes_with_confidence = get_entity_to_targets_raw(
        client=client,
        background_gene_ids=background_gene_ids,
    )
    return filter_gene_set_confidences(
        genes_with_confidence,
        minimum_belief=minimum_belief,
        minimum_evidence_count=minimum_evidence_count,
    )


def get_entity_to_targets_raw(
    client: Optional[Neo4jClient] = None,
    background_gene_ids: Optional[Iterable[str]] = None,
    use_sqlite_cache: bool = True,
    limit: Optional[int] = None,
    sqlite_db_path: Union[Path, str] = SQLITE_CACHE_PATH,
) -> Dict[Tuple[str, str], Dict[str, Tuple[float, int]]]:
    """Get all regulator to target relationships

    Parameters
    ----------
    client :
        The Neo4j client.
    background_gene_ids :
        List of HGNC gene identifiers for the background gene set. If not
        given, all genes with HGNC IDs are used as the background.
    use_sqlite_cache :
        If True, use the SQLite cache if it exists. Default: True.
    sqlite_db_path :
        Path to the SQLite database to use for caching. Default:
        APP_CACHE_MODULE found in `indra_cogex.apps.constants`.
    limit :
        If given, limit the number of entities returned to this number.

    Returns
    -------
    :
        A dictionary whose keys are 2-tuples of CURIE and name of each
        entity and whose values are dicts of HGNC gene identifiers (as strings)
        pointing to the maximum belief and evidence count associated with the
        given HGNC gene.
    """
    if sqlite_db_path.exists() and use_sqlite_cache:
        genes_with_confidence = get_sqlite_genes_with_confidence_cache(
            "entity_to_targets",
            background_gene_ids=background_gene_ids,
            sqlite_db_path=sqlite_db_path,
            limit=limit
        )
    else:
        query = dedent(
            f"""\
            MATCH (regulator:BioEntity)-[r:indra_rel]->(gene:BioEntity)
            WHERE
                gene.id STARTS WITH "hgnc"                  // Collecting human genes only
                AND NOT gene.obsolete                       // Skip obsolete
                AND r.stmt_type <> "Complex"                // Ignore complexes since they are non-directional
                AND NOT regulator.id STARTS WITH "uniprot"  // This is a simple way to ignore non-human proteins
            RETURN
                regulator.id,
                regulator.name,
                collect([gene.id, r.belief, r.evidence_count])
        """
        )
        if limit is not None:
            query += f"\nLIMIT {limit}"
        genes_with_confidence = collect_genes_with_confidence(
            client=client,
            query=query,
            background_gene_ids=background_gene_ids,
        )
    return genes_with_confidence


@autoclient()
def get_entity_to_regulators(
    *,
    client: Neo4jClient,
    background_gene_ids: Optional[Iterable[str]] = None,
    minimum_evidence_count: Optional[int] = 1,
    minimum_belief: Optional[float] = 0.0,
) -> Dict[Tuple[str, str], Set[str]]:
    """Get a mapping from each entity in the INDRA database to the set of
    human genes that are causally upstream of it.

    Parameters
    ----------
    client :
        The Neo4j client.
    background_gene_ids :
        List of HGNC gene identifiers for the background gene set. If not
        given, all genes with HGNC IDs are used as the background.
    minimum_evidence_count :
        The minimum number of evidences for a relationship to count it as a
        regulator. Defaults to 1 (i.e., cutoff not applied.
    minimum_belief :
        The minimum belief for a relationship to count it as a regulator.
        Defaults to 0.0 (i.e., cutoff not applied).

    Returns
    -------
    :
        A dictionary whose keys are 2-tuples of CURIE and name of each
        entity and whose values are sets of HGNC gene identifiers (as strings).
    """
    genes_with_confidence = get_entity_to_regulators_raw(
        client=client,
        background_gene_ids=background_gene_ids,
    )
    return filter_gene_set_confidences(
        genes_with_confidence,
        minimum_belief=minimum_belief,
        minimum_evidence_count=minimum_evidence_count,
    )


@autoclient()
def get_entity_to_regulators_raw(
    *,
    client: Neo4jClient,
    background_gene_ids: Optional[Iterable[str]] = None,
    use_sqlite_cache: bool = True,
    limit: Optional[int] = None,
    sqlite_db_path: Union[Path, str] = SQLITE_CACHE_PATH,
) -> Dict[Tuple[str, str], Dict[str, Tuple[float, int]]]:
    """Get all target to regulator relationships

    Parameters
    ----------
    client :
        The Neo4j client.
    background_gene_ids :
        List of HGNC gene identifiers for the background gene set. If not
        given, all genes with HGNC IDs are used as the background.
    use_sqlite_cache :
        If True, use the SQLite cache if it exists. Default: True.
    sqlite_db_path :
        Path to the SQLite database to use for caching. Default:
        APP_CACHE_MODULE found in `indra_cogex.apps.constants`.
    limit :
        If given, limit the number of entities returned to this number.

    Returns
    -------
    :
        A dictionary whose keys are 2-tuples of CURIE and name of each
        entity and whose values are dicts of HGNC gene identifiers (as strings)
        pointing to the maximum belief and evidence count associated with the
        given HGNC gene.
    """
    if sqlite_db_path.exists() and use_sqlite_cache:
        genes_with_confidence = get_sqlite_genes_with_confidence_cache(
            "entity_to_regulators",
            background_gene_ids=background_gene_ids,
            sqlite_db_path=sqlite_db_path,
            limit=limit
        )
    else:
        query = dedent(
            f"""\
            MATCH (gene:BioEntity)-[r:indra_rel]->(target:BioEntity)
            WHERE
                gene.id STARTS WITH "hgnc"               // Collecting human genes only
                AND NOT gene.obsolete                    // Skip obsolete
                AND r.stmt_type <> "Complex"             // Ignore complexes since they are non-directional
                AND NOT target.id STARTS WITH "uniprot"  // This is a simple way to ignore non-human proteins
            RETURN
                target.id,
                target.name,
                collect([gene.id, r.belief, r.evidence_count])
        """
        )
        if limit is not None:
            query += f"\nLIMIT {limit}"
        genes_with_confidence = collect_genes_with_confidence(
            client=client,
            query=query,
            background_gene_ids=background_gene_ids,
        )
    return genes_with_confidence

def minimum_evidence_helper(
    minimum_evidence_count: Optional[float] = None, name: str = "r"
) -> str:
    if minimum_evidence_count is None or minimum_evidence_count == 1:
        return ""
    return f"AND {name}.evidence_count >= {minimum_evidence_count}"


def minimum_belief_helper(
    minimum_belief: Optional[float] = None, name: str = "r"
) -> str:
    if minimum_belief is None or minimum_belief == 0.0:
        return ""
    return f"AND {name}.belief >= {minimum_belief}"


# TODO should this include other statement types? is the mechanism linker applied before
#  importing the database into CoGEx?

POSITIVE_STMT_TYPES = ["Activation", "IncreaseAmount"]
NEGATIVE_STMT_TYPES = ["Inhibition", "DecreaseAmount"]


# FIXME should we further limit this query to only a certain type of entities,
#  or split it up at least? (e.g., specific analysis for chemicals, genes, etc.)


def _query(
    stmt_types: Iterable[str],
    minimum_evidence_count: Optional[int] = None,
    minimum_belief: Optional[float] = None,
    limit: Optional[int] = None,
) -> str:
    """Return a query over INDRA relations f the given statement types."""
    query_range = ", ".join(f'"{stmt_type}"' for stmt_type in sorted(stmt_types))
    if minimum_evidence_count is None or minimum_evidence_count == 1:
        evidence_line = ""
    else:
        evidence_line = f"AND r.evidence_count >= {minimum_evidence_count}"
    if minimum_belief is None or minimum_belief == 0.0:
        belief_line = ""
    else:
        belief_line = f"AND r.belief >= {minimum_belief}"
    query_str = dedent(
        f"""\
        MATCH (regulator:BioEntity)-[r:indra_rel]->(gene:BioEntity)
        WHERE gene.id STARTS WITH "hgnc"                // Collecting human genes only
            AND r.stmt_type in [{query_range}]          // Ignore complexes since they are non-directional
            AND NOT regulator.id STARTS WITH "uniprot"  // This is a simple way to ignore non-human proteins
            AND NOT gene.obsolete                       // Skip obsolete
            {evidence_line}
            {belief_line}
        RETURN
            regulator.id,
            regulator.name,
            collect([gene.id, r.belief, r.evidence_count])
    """
    )
    if limit is not None:
        query_str += f"\nLIMIT {limit}"
    return query_str


@autoclient()
def get_positive_stmt_sets(
    *,
    client: Neo4jClient,
    background_gene_ids: Optional[Iterable[str]] = None,
    minimum_evidence_count: Optional[int] = 1,
    minimum_belief: Optional[float] = 0.0,
) -> Dict[Tuple[str, str], Set[str]]:
    """Get a mapping from each entity in the INDRA database to the set of
    entities that are causally downstream of human genes via an "activates"
    or "increases amount of" relationship.

    Parameters
    ----------
    client :
        The Neo4j client.
    background_gene_ids :
        List of HGNC gene identifiers for the background gene set. If not
        given, all genes with HGNC IDs are used as the background.
    minimum_evidence_count :
        The minimum number of evidences for a relationship.
        Defaults to 1 (i.e., cutoff not applied.
    minimum_belief :
        The minimum belief for a relationship.
        Defaults to 0.0 (i.e., cutoff not applied).

    Returns
    -------
    :
        A dictionary whose keys are 2-tuples of CURIE and name of each entity
        and whose values are sets of HGNC gene identifiers (as strings)
    """
    res = get_positive_stmt_sets_raw(
        client=client,
        background_gene_ids=background_gene_ids,
    )

    return filter_gene_set_confidences(
        res,
        minimum_belief=minimum_belief,
        minimum_evidence_count=minimum_evidence_count,
    )


@autoclient()
def get_positive_stmt_sets_raw(
    *,
    client: Neo4jClient,
    background_gene_ids: Optional[Iterable[str]] = None,
    use_sqlite_cache: bool = True,
    limit: Optional[int] = None,
    sqlite_db_path: Union[Path, str] = SQLITE_CACHE_PATH,
) -> Dict[Tuple[str, str], Dict[str, Tuple[float, int]]]:
    """Get all positive regulator to target relationships

    Parameters
    ----------
    client :
        The Neo4j client.
    background_gene_ids :
        List of HGNC gene identifiers for the background gene set. If not
        given, all genes with HGNC IDs are used as the background.
    use_sqlite_cache :
        If True, use the SQLite cache if it exists. Default: True.
    sqlite_db_path :
        Path to the SQLite database to use for caching. Default:
        APP_CACHE_MODULE found in `indra_cogex.apps.constants`.
    limit :
        If given, limit the number of entities returned to this number.

    Returns
    -------
    :
        A dictionary whose keys are 2-tuples of CURIE and name of each
        entity and whose values are dicts of HGNC gene identifiers (as strings)
        pointing to the maximum belief and evidence count associated with the
        given HGNC gene.
    """
    if sqlite_db_path.exists() and use_sqlite_cache:
        genes_with_confidence = get_sqlite_genes_with_confidence_cache(
            "positive_statements",
            background_gene_ids=background_gene_ids,
            sqlite_db_path=sqlite_db_path,
            limit=limit
        )
    else:
        genes_with_confidence = collect_genes_with_confidence(
            query=_query(POSITIVE_STMT_TYPES, limit=limit),
            client=client,
            background_gene_ids=background_gene_ids,
        )
    return genes_with_confidence


@autoclient()
def get_negative_stmt_sets(
    *,
    client: Neo4jClient,
    background_gene_ids: Optional[Iterable[str]] = None,
    minimum_evidence_count: Optional[int] = 1,
    minimum_belief: Optional[float] = 0.0,
) -> Dict[Tuple[str, str], Set[str]]:
    """Get a mapping from each entity in the INDRA database to the set of
    entities that are causally downstream of human genes via an "inhibits"
    or "decreases amount of" relationship.

    Parameters
    ----------
    client :
        The Neo4j client.
    background_gene_ids :
        List of HGNC gene identifiers for the background gene set. If not
        given, all genes with HGNC IDs are used as the background.
    minimum_evidence_count :
        The minimum number of evidences for a relationship.
        Defaults to 1 (i.e., cutoff not applied).
    minimum_belief :
        The minimum belief for a relationship.
        Defaults to 0.0 (i.e., cutoff not applied).

    Returns
    -------
    :
        A dictionary whose keys are 2-tuples of CURIE and name of each entity
        and whose values are sets of HGNC gene identifiers (as strings)
    """
    res = get_negative_stmt_sets_raw(
        client=client,
        background_gene_ids=background_gene_ids,
    )
    return filter_gene_set_confidences(
        res,
        minimum_belief=minimum_belief,
        minimum_evidence_count=minimum_evidence_count,
    )


@autoclient()
def get_negative_stmt_sets_raw(
    *,
    client: Neo4jClient,
    background_gene_ids: Optional[Iterable[str]] = None,
    use_sqlite_cache: bool = True,
    limit: Optional[int] = None,
    sqlite_db_path: Union[Path, str] = SQLITE_CACHE_PATH,
) -> Dict[Tuple[str, str], Dict[str, Tuple[float, int]]]:
    """Get all negative regulator to target relationships

    Parameters
    ----------
    client :
        The Neo4j client.
    background_gene_ids :
        List of HGNC gene identifiers for the background gene set. If not
        given, all genes with HGNC IDs are used as the background.
    use_sqlite_cache :
        If True, use the SQLite cache if it exists. Default: True.
    sqlite_db_path :
        Path to the SQLite database to use for caching. Default:
        APP_CACHE_MODULE found in `indra_cogex.apps.constants`.
    limit :
        If given, limit the number of entities returned to this number.

    Returns
    -------
    :
        A dictionary whose keys are 2-tuples of CURIE and name of each
        entity and whose values are dicts of HGNC gene identifiers (as strings)
        pointing to the maximum belief and evidence count associated with the
        given HGNC gene.
    """
    if sqlite_db_path.exists() and use_sqlite_cache:
        genes_with_confidence = get_sqlite_genes_with_confidence_cache(
            "negative_statements",
            background_gene_ids=background_gene_ids,
            sqlite_db_path=sqlite_db_path,
            limit=limit
        )
    else:
        genes_with_confidence = collect_genes_with_confidence(
            query=_query(NEGATIVE_STMT_TYPES, limit=limit),
            client=client,
            background_gene_ids=background_gene_ids,
        )
    return genes_with_confidence


def _normalize_target_id(curie: str) -> str:
    """Only normalize if it's likely an Entrez gene or HGNC numeric code"""
    if curie.lower().startswith("hgnc:") or curie.isdigit():
        return norm_id("HGNC", curie.split(":")[-1])
    return curie.lower()


def get_statement_metadata_for_pairs(
    regulator_gene_pairs: List[Tuple[str, str]],
    minimum_belief: float = 0.0,
    minimum_evidence: Optional[int] = None,
    is_downstream: bool = False,
    allowed_stmt_types: Optional[List[str]] = None,
    *,
    client: Neo4jClient,
) -> Dict[str, List[Dict]]:
    """Fetch INDRA statement metadata (including stmt_type) for given (regulator, gene) pairs.

    Parameters
    ----------
    regulator_gene_pairs : List[Tuple[str, str]]
        A list of (regulator_curie, gene_curie) pairs.
    minimum_belief : float
        Minimum belief score to include a statement.
    minimum_evidence : Optional[int]
        Minimum evidence count to include a statement.
    is_downstream : bool
        Whether the direction is downstream (gene -> regulator).
    allowed_stmt_types : Optional[List[str]]
        List of statement types to filter by. If None, includes all types.
    client : Neo4jClient
        Neo4j client instance.

    Returns
    -------
    Dict[str, List[Dict]]
        Mapping from regulator_id to list of statement metadata dicts.
    """
    if not regulator_gene_pairs:
        return {}

    metadata_by_regulator = defaultdict(list)

    # Normalize IDs
    regulator_gene_pairs = [
        (reg, norm_id("HGNC", gene.split(":")[-1]))
        for reg, gene in regulator_gene_pairs
    ]

    # Pre-built queries instead of f-string conditionals
    stmt_type_filter = ""
    if allowed_stmt_types is not None:
        stmt_type_filter = "AND r.stmt_type IN $allowed_stmt_types"

    # Pre-defined query templates
    UPSTREAM_QUERY = f"""
            UNWIND $pairs AS pair
            WITH pair[0] AS regulator_id, pair[1] AS gene_id
            MATCH (reg:BioEntity {{id: regulator_id}})-[r:indra_rel]->(gene:BioEntity {{id: gene_id}})
            WHERE r.belief > $minimum_belief {stmt_type_filter}
            RETURN regulator_id, gene_id, r.stmt_hash AS stmt_hash, r.belief AS belief,
                   r.evidence_count AS evidence_count, r.stmt_type AS stmt_type, gene.name AS gene_name
        """

    DOWNSTREAM_QUERY = f"""
            UNWIND $pairs AS pair
            WITH pair[0] AS regulator_id, pair[1] AS gene_id
            MATCH (gene:BioEntity {{id: gene_id}})-[r:indra_rel]->(reg:BioEntity {{id: regulator_id}})
            WHERE r.belief > $minimum_belief {stmt_type_filter}
            RETURN regulator_id, gene_id, r.stmt_hash AS stmt_hash, r.belief AS belief,
                   r.evidence_count AS evidence_count, r.stmt_type AS stmt_type, gene.name AS gene_name
        """

    # Select appropriate query
    query = DOWNSTREAM_QUERY if is_downstream else UPSTREAM_QUERY

    params = {
        "pairs": regulator_gene_pairs,
        "minimum_belief": minimum_belief
    }

    if allowed_stmt_types is not None:
        params["allowed_stmt_types"] = allowed_stmt_types

    results = client.query_tx(query, **params)

    for reg, gene, stmt_hash, belief, ev_count, stmt_type, gene_name in results:
        if minimum_evidence and ev_count < minimum_evidence:
            continue

        metadata_by_regulator[reg].append({
            "gene": gene,
            "stmt_hash": stmt_hash,
            "belief": belief,
            "evidence_count": ev_count,
            "stmt_type": stmt_type or "indra_rel",
            "gene_name": gene_name
        })

    return dict(metadata_by_regulator)


def enrich_with_optimized_metadata(
    results: Dict[str, pd.DataFrame],
    gene_set: set,
    client: Neo4jClient,
    minimum_belief: float,
    minimum_evidence_count: int
) -> Dict[str, pd.DataFrame]:
    """Metadata enrichment for INDRA results in a results dictionary.

    OPTIMIZED metadata enrichment - replaces the cartesian product bottleneck

    Instead of creating R by G pairs and querying each individually,
    we do ONE smart query to get all existing relationships.

    Parameters
    ----------
    results :
        A dictionary of results, mapping result type to DataFrame.
    gene_set :
        The set of gene CURIEs (HGNC) to consider as targets.
    client :
        The Neo4j client.
    minimum_belief :
        Minimum belief score to include a relationship.
    minimum_evidence_count :
        Minimum evidence count to include a relationship.

    Returns
    -------
    :
        The input results dictionary with INDRA results enriched with
        statement metadata.
    """

    # Get all INDRA results that need metadata
    indra_results = {k: v for k, v in results.items() if k.startswith("indra-")}

    if not indra_results:
        return results

    # Collect all unique regulators across all INDRA results
    all_regulators = set()
    for df in indra_results.values():
        if isinstance(df, pd.DataFrame) and not df.empty:
            all_regulators.update(df["curie"].tolist())

    if not all_regulators:
        return results

    # SINGLE OPTIMIZED QUERY instead of cartesian product
    metadata_cache = get_all_relationships_single_query(
        regulators=list(all_regulators),
        genes=list(gene_set),
        client=client,
        minimum_belief=minimum_belief,
        minimum_evidence_count=minimum_evidence_count
    )

    # Apply metadata to each INDRA result
    for result_key, df in indra_results.items():
        if isinstance(df, pd.DataFrame) and not df.empty:
            is_downstream = result_key == "indra-downstream"

            # Map statements efficiently
            df["statements"] = df["curie"].apply(
                lambda curie: metadata_cache.get((curie, is_downstream), [])
            )
            results[result_key] = df

    return results


def get_all_relationships_single_query(
    regulators: List[str],
    genes: List[str],
    client: Neo4jClient,
    minimum_belief: float = 0.0,
    minimum_evidence_count: int = 1
) -> Dict[Tuple[str, bool], List[Dict]]:
    """Query to get all regulator-gene relationships

    Parameters
    ----------
    regulators :
        List of regulator CURIEs.
    genes :
        List of gene CURIEs (HGNC).
    client :
        The Neo4j client.
    minimum_belief :
        Minimum belief score to include a relationship.
    minimum_evidence_count :
        Minimum evidence count to include a relationship.

    Returns
    -------
    :
        A dictionary whose keys are (regulator_id, is_downstream) tuples and
        whose values are lists of statement metadata dictionaries.
    """

    # Normalize gene IDs
    normalized_genes = [norm_id("HGNC", gene.split(":")[-1]) for gene in genes]

    # SINGLE QUERY to get ALL relationships in both directions
    query = """
    MATCH (reg:BioEntity)-[r:indra_rel]-(gene:BioEntity)
    WHERE reg.id IN $regulators 
      AND gene.id IN $genes
      AND r.belief > $minimum_belief
      AND r.evidence_count >= $minimum_evidence_count
    RETURN reg.id as regulator_id, 
           gene.id as gene_id,
           r.stmt_hash as stmt_hash, 
           r.belief as belief,
           r.evidence_count as evidence_count, 
           r.stmt_type as stmt_type,
           gene.name as gene_name,
           CASE 
             WHEN startNode(r) = reg THEN false 
             ELSE true 
           END as is_downstream
    """

    # Execute single query
    results = client.query_tx(
        query,
        regulators=regulators,
        genes=normalized_genes,
        minimum_belief=minimum_belief,
        minimum_evidence_count=minimum_evidence_count
    )

    # Organize results by (regulator_id, is_downstream)
    metadata_cache = defaultdict(list)

    for reg_id, gene_id, stmt_hash, belief, ev_count, stmt_type, gene_name, is_downstream in results:
        key = (reg_id, is_downstream)

        metadata_cache[key].append({
            "gene": gene_id,
            "stmt_hash": stmt_hash,
            "belief": belief,
            "evidence_count": ev_count,
            "stmt_type": stmt_type or "indra_rel",
            "gene_name": gene_name
        })

    return dict(metadata_cache)


def get_mouse_cache(force_cache_refresh: bool = False):
    import pyobo
    _ = pyobo.get_name_id_mapping(
        "mgi", force=force_cache_refresh, version=PYOBO_RESOURCE_FILE_VERSIONS.get("mgi")
    )


def get_rat_cache(force_cache_refresh: bool = False):
    import pyobo
    _ = pyobo.get_name_id_mapping(
        "rgd", force=force_cache_refresh, version=PYOBO_RESOURCE_FILE_VERSIONS.get("rgd")
    )


def build_sqlite_cache(
    db_path: Path = SQLITE_CACHE_PATH,
    force: bool = False,
    limit: Optional[int] = None,
    return_cache: bool = False,
) -> Optional[Dict[str, Dict]]:
    """Build the SQLite cache for CoGEx.

    Parameters
    ----------
    db_path :
        The path to the SQLite database file. Default: SQLITE_CACHE_PATH.
    force :
        If True, the current cache will be ignored and the database will be
        rebuilt. The current database will be overwritten. Default: False.
    limit :
        If given, limits the number of rows processed from Neo4j for each
        dataset. This is useful for testing and development. Default: None.
    return_cache :
        If True, returns the results from the graph database queries in a
        dictionary. This is useful for testing. Default: False.

    Returns
    -------
    :
        If return_cache is True, returns a dictionary with the data used to
        populate the SQLite cache. Otherwise, returns None.
    """
    if db_path.exists() and not force:
        logger.info(f"SQLite cache already exists at {db_path}. Skipping build.")
        return

    if force:
        logger.info(f"Force rebuilding SQLite cache at {db_path}.")
        db_path.unlink(missing_ok=True)

    client = Neo4jClient()
    if not client.ping():
        raise RuntimeError("Cannot connect to Neo4j database. Cannot build SQLite cache.")

    # Table for (curie, name) to gene set mapping
    # Use for GO, Reactome, WikiPathways, Phenotypes
    gene_set_table = f"""
    CREATE TABLE {SQLITE_GENE_SET_TABLE} (
        cache_name TEXT NOT NULL,     -- which dataset (1 of 5)
        curie TEXT NOT NULL,          -- key part 1
        name TEXT NOT NULL,           -- key part 2
        value TEXT NOT NULL,          -- one entry in the set
        PRIMARY KEY (cache_name, curie, name, value)
    );
    """

    regulator_target_table = f"""
    CREATE TABLE {SQLITE_GENES_WITH_CONFIDENCE_TABLE} (
        cache_name TEXT NOT NULL,     -- which dataset (1 of 4)
        curie TEXT NOT NULL,          -- outer key part 1
        name TEXT NOT NULL,           -- outer key part 2
        inner_key TEXT NOT NULL,      -- the nested key
        belief REAL NOT NULL,         -- float value
        ev_count INTEGER NOT NULL,    -- int value
        PRIMARY KEY (cache_name, curie, name, inner_key)
    );
    """

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(gene_set_table)
    cursor.execute(regulator_target_table)
    conn.commit()
    conn.close()
    logger.info(f"Built SQLite cache at {db_path}.")

    # Populate the cache
    logger.info("Warming up bioontology...")
    bio_ontology.initialize()
    cache_data = {}
    if return_cache:
        cache_data[SQLITE_GENE_SET_TABLE] = {}
        cache_data[SQLITE_GENES_WITH_CONFIDENCE_TABLE] = {}

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    logger.info("Populating SQLite gene set cache")
    for cache_name, func in tqdm(
        gene_set_table_datasets.items(), desc="Populating gene set cache"
    ):
        data = func(use_sqlite_cache=False, limit=limit)

        # Store in return cache if requested
        if return_cache:
            cache_data[SQLITE_GENE_SET_TABLE][cache_name] = data

        # Prepare data for insertion
        to_insert = []
        sorted_keys = sorted(data.keys(), key=lambda x: (x[0], x[1]))
        for (curie, name) in sorted_keys:
            values = data[(curie, name)]
            for value in sorted(values):
                to_insert.append((cache_name, curie, name, value))

        # Insert data into the database
        cursor.executemany(
            f"INSERT OR IGNORE INTO {SQLITE_GENE_SET_TABLE} "
            f"(cache_name, curie, name, value) VALUES (?, ?, ?, ?);",
            to_insert
        )
        conn.commit()

    logger.info("Populating SQLite genes with confidence cache")
    for cache_name, func in tqdm(
        genes_with_confidence_datasets.items(),
        desc="Populating genes with confidence cache"
    ):
        data = func(use_sqlite_cache=False, limit=limit)

        # Store in return cache if requested
        if return_cache:
            cache_data[SQLITE_GENES_WITH_CONFIDENCE_TABLE][cache_name] = data

        if cache_name == "kinase_phosphosites":
            # Make the inner key 3-tuple a string (curie, name, site)
            flat_key_data = {}
            for (curie, name), values in data.items():
                flat_key_data[(curie, name)] = {}
                for (gene_curie, gene_name, site), (belief, ev_count) in values.items():
                    inner_key = f"{gene_curie}|{gene_name}|{site}"
                    flat_key_data[(curie, name)][inner_key] = (belief, ev_count)
            data = flat_key_data

        # Prepare data for insertion
        to_insert = []
        sorted_keys = sorted(data.keys(), key=lambda x: (x[0], x[1]))
        for (curie, name) in sorted_keys:
            inner_dict = data[(curie, name)]
            for inner_key in sorted(inner_dict.keys()):
                belief, ev_count = inner_dict[inner_key]
                to_insert.append((cache_name, curie, name, inner_key, belief, ev_count))

        # Insert data into the database
        cursor.executemany(
            f"INSERT OR IGNORE INTO {SQLITE_GENES_WITH_CONFIDENCE_TABLE} "
            f"(cache_name, curie, name, inner_key, belief, ev_count) "
            f"VALUES (?, ?, ?, ?, ?, ?);",
            to_insert
        )
        conn.commit()

    conn.close()
    logger.info(f"Finished building and populating SQLite cache at {db_path}.")

    return cache_data or None


gene_set_table_datasets = {
    # Returns set of gene IDs
    "go": get_go,
    "reactome": get_reactome,
    "wikipathways": get_wikipathways,
    "phenotypes": get_phenotype_gene_sets,
}
genes_with_confidence_datasets = {
    # Returns dict of gene ID to (belief, evidence_count)
    "entity_to_targets": get_entity_to_targets_raw,
    "entity_to_regulators": get_entity_to_regulators_raw,
    "positive_statements": get_positive_stmt_sets_raw,
    "negative_statements": get_negative_stmt_sets_raw,
    "kinase_phosphosites": get_kinase_phosphosites_raw,
}


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Build caches for gene set enrichment analysis."
    )
    parser.add_argument(
        "-f", "--force-refresh",
        action="store_true",
        help="Force a refresh of the cache.",
    )
    args = parser.parse_args()
    build_sqlite_cache(force=args.force_refresh)

    # Build the pyobo name-id mapping caches. Skip force refresh since the data
    # isn't from CoGEx, rather change the version to download a new cache.
    # See PYOBO_RESOURCE_FILE_VERSIONS in indra_cogex/apps/constants.py
    # NOTE: This will build all files for the pyobo caches, but we only need names.tsv
    # Instead, we copy names.tsv files during docker build for each resource.
    # get_mouse_cache()
    # get_rat_cache()
