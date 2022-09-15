# -*- coding: utf-8 -*-

"""Utilities for getting gene sets."""

import logging
import pickle
from collections import defaultdict
from pathlib import Path
from textwrap import dedent
from typing import Any, DefaultDict, Dict, Mapping, Optional, Set, Tuple, TypeVar

import pystow
from indra.databases.identifiers import get_ns_id_from_identifiers
from indra.ontology.bio import bio_ontology

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
]

logger = logging.getLogger(__name__)

X = TypeVar("X")
Y = TypeVar("Y")
APP_CACHE_MODULE = pystow.module("indra", "cogex", "app_cache")
GO_GENE_SET_PATH = APP_CACHE_MODULE.join(name="go.pkl")
WIKIPATHWAYS_GENE_SET_PATH = APP_CACHE_MODULE.join(name="wiki.pkl")
REACTOME_GENE_SETS_PATH = APP_CACHE_MODULE.join(name="reactome.pkl")
HPO_GENE_SETS_PATH = APP_CACHE_MODULE.join(name="hpo.pkl")
TO_REGULATORS_GENE_SETS_PATH = APP_CACHE_MODULE.join(name="to_regs.pkl")
TO_TARGETS_GENE_SETS_PATH = APP_CACHE_MODULE.join(name="to_targets.pkl")
NEGATIVES_GENE_SETS_PATH = APP_CACHE_MODULE.join(name="negatives.pkl")
POSITIVES_GENE_SETS_PATH = APP_CACHE_MODULE.join(name="positives.pkl")

GENE_SET_CACHE: Dict[str, Any] = {}


@autoclient()
def collect_gene_sets(
    query: str,
    *,
    client: Neo4jClient,
    include_ontology_children: bool = False,
    cache_file: Optional[Path] = None,
) -> Dict[Tuple[str, str], Set[str]]:
    """Collect gene sets based on the given query.

    Parameters
    ----------
    query:
        A cypher query
    client :
        The Neo4j client.
    include_ontology_children :
        If True, extend the gene set associations with associations from
        child terms using the indra ontology
    cache_file :
        The path to the cache file.

    Returns
    -------
    :
        A dictionary whose keys that are 2-tuples of CURIE and name of each queried
        item and whose values are sets of HGNC gene identifiers (as strings)
    """
    if cache_file is not None and cache_file.as_posix() in GENE_SET_CACHE:
        logger.info("Returning %s from in-memory cache" % cache_file.as_posix())
        return GENE_SET_CACHE[cache_file.as_posix()]
    elif cache_file is not None and cache_file.exists():
        logger.info("Loading %s" % cache_file.as_posix())
        with open(cache_file, "rb") as fh:
            res = pickle.load(fh)
        GENE_SET_CACHE[cache_file.as_posix()] = res
        return res

    if cache_file is not None:
        logger.info(
            "Running new query and caching results into %s" % cache_file.as_posix()
        )
    curie_to_hgnc_ids: DefaultDict[Tuple[str, str], Set[str]] = defaultdict(set)
    res = client.query_tx(query)
    if res is None:
        raise ValueError
    for curie, name, hgnc_curies in res:
        curie_to_hgnc_ids[curie, name].update(
            hgnc_curie.lower().replace("hgnc:", "")
            if hgnc_curie.lower().startswith("hgnc:")
            else hgnc_curie.lower()
            for hgnc_curie in hgnc_curies
        )
    res = dict(curie_to_hgnc_ids)

    if include_ontology_children:
        extend_by_ontology(res)

    if cache_file is not None:
        with open(cache_file, "wb") as fh:
            pickle.dump(res, fh)
        GENE_SET_CACHE[cache_file.as_posix()] = res
    return res


def extend_by_ontology(gene_set_mapping: Dict[Tuple[str, str], Set[str]]):
    """Extend the gene set mapping by ontology."""

    bio_ontology.initialize()
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
    cache_file: Optional[Path] = None,
    client: Neo4jClient,
) -> Dict[Tuple[str, str], Dict[str, Tuple[float, int]]]:
    """Collect gene sets based on the given query.

    Parameters
    ----------
    query:
        A cypher query
    client :
        The Neo4j client.

    Returns
    -------
    :
        A dictionary whose keys that are 2-tuples of CURIE and name of each queried
        item and whose values are dicts of HGNC gene identifiers (as strings)
        pointing to the maximum belief and evidence count associated with
        the given HGNC gene.
    """
    if cache_file is not None and cache_file.as_posix() in GENE_SET_CACHE:
        logger.info("Returning %s from in-memory cache" % cache_file.as_posix())
        return GENE_SET_CACHE[cache_file.as_posix()]
    elif cache_file is not None and cache_file.exists():
        logger.info("Loading %s" % cache_file.as_posix())
        with open(cache_file, "rb") as fh:
            curie_to_hgnc_ids = pickle.load(fh)
        GENE_SET_CACHE[cache_file.as_posix()] = curie_to_hgnc_ids
        return curie_to_hgnc_ids

    if cache_file is not None:
        logger.info(
            "Running new query and caching results into %s" % cache_file.as_posix()
        )
    curie_to_hgnc_ids = defaultdict(dict)
    max_beliefs: Dict[Tuple[str, str, str], float] = {}
    max_ev_counts: Dict[Tuple[str, str, str], int] = {}
    for result in client.query_tx(query):
        curie = result[0]
        name = result[1]
        hgnc_ids = set()
        for hgnc_curie, belief, ev_count in result[2]:
            hgnc_id = (
                hgnc_curie.lower().replace("hgnc:", "")
                if hgnc_curie.lower().startswith("hgnc:")
                else hgnc_curie.lower()
            )
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

    if cache_file is not None:
        with open(cache_file, "wb") as fh:
            pickle.dump(curie_to_hgnc_ids, fh)
        GENE_SET_CACHE[cache_file.as_posix()] = curie_to_hgnc_ids
    return curie_to_hgnc_ids


@autoclient(cache=True)
def get_go(*, client: Neo4jClient) -> Dict[Tuple[str, str], Set[str]]:
    """Get GO gene sets.

    Parameters
    ----------
    client :
        The Neo4j client.

    Returns
    -------
    :
        A dictionary whose keys that are 2-tuples of CURIE and name of each GO term
        and whose values are sets of HGNC gene identifiers (as strings)
    """
    query = dedent(
        """\
        MATCH (gene:BioEntity)-[:associated_with]->(term:BioEntity)
        RETURN term.id, term.name, collect(gene.id) as gene_curies;
    """
    )
    return collect_gene_sets(
        client=client,
        query=query,
        cache_file=GO_GENE_SET_PATH,
        include_ontology_children=True,
    )


@autoclient()
def get_wikipathways(*, client: Neo4jClient) -> Dict[Tuple[str, str], Set[str]]:
    """Get WikiPathways gene sets.

    Parameters
    ----------
    client :
        The Neo4j client.

    Returns
    -------
    :
        A dictionary whose keys that are 2-tuples of CURIE and name of each WikiPathway
        pathway and whose values are sets of HGNC gene identifiers (as strings)
    """
    query = dedent(
        """\
        MATCH (pathway:BioEntity)-[:haspart]->(gene:BioEntity)
        WHERE pathway.id STARTS WITH "wikipathways" and gene.id STARTS WITH "hgnc"
        RETURN pathway.id, pathway.name, collect(gene.id);
    """
    )
    return collect_gene_sets(
        client=client, query=query, cache_file=WIKIPATHWAYS_GENE_SET_PATH
    )


@autoclient()
def get_reactome(*, client: Neo4jClient) -> Dict[Tuple[str, str], Set[str]]:
    """Get Reactome gene sets.

    Parameters
    ----------
    client :
        The Neo4j client.

    Returns
    -------
    :
        A dictionary whose keys that are 2-tuples of CURIE and name of each Reactome
        pathway and whose values are sets of HGNC gene identifiers (as strings)
    """
    query = dedent(
        """\
        MATCH (pathway:BioEntity)-[:haspart]-(gene:BioEntity)
        WHERE pathway.id STARTS WITH "reactome" and gene.id STARTS WITH "hgnc"
        RETURN pathway.id, pathway.name, collect(gene.id);
    """
    )
    return collect_gene_sets(
        client=client, query=query, cache_file=REACTOME_GENE_SETS_PATH
    )


@autoclient()
def get_phenotype_gene_sets(*, client: Neo4jClient) -> Dict[Tuple[str, str], Set[str]]:
    """Get HPO phenotype gene sets.

    Parameters
    ----------
    client :
        The Neo4j client.

    Returns
    -------
    :
        A dictionary whose keys that are 2-tuples of CURIE and name of each phenotype
        gene set and whose values are sets of HGNC gene identifiers (as strings)
    """
    query = dedent(
        """\
        MATCH (s:BioEntity)-[:phenotype_has_gene]-(gene:BioEntity)
        WHERE s.id STARTS WITH "hp" and gene.id STARTS WITH "hgnc"
        RETURN s.id, s.name, collect(gene.id);
    """
    )
    return collect_gene_sets(client=client, query=query, cache_file=HPO_GENE_SETS_PATH)


def filter_gene_set_confidences(
    data: Dict[X, Dict[Y, Tuple[float, int]]],
    minimum_belief: Optional[float] = None,
    minimum_evidence_count: Optional[int] = None,
) -> Mapping[X, Set[Y]]:
    """Filter the confidences from a dictionary."""
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


@autoclient()
def get_entity_to_targets(
    *,
    client: Neo4jClient,
    minimum_evidence_count: Optional[int] = 1,
    minimum_belief: Optional[float] = 0.0,
) -> Dict[Tuple[str, str], Set[str]]:
    """Get a mapping from each entity in the INDRA database to the set of
    human genes that it regulates.

    Parameters
    ----------
    client :
        The Neo4j client.
    minimum_evidence_count :
        The minimum number of evidences for a relationship to count it as a regulator.
        Defaults to 1 (i.e., cutoff not applied.
    minimum_belief :
        The minimum belief for a relationship to count it as a regulator.
        Defaults to 0.0 (i.e., cutoff not applied).

    Returns
    -------
    :
        A dictionary whose keys that are 2-tuples of CURIE and name of each entity
        and whose values are sets of HGNC gene identifiers (as strings)
    """
    query = dedent(
        f"""\
        MATCH (regulator:BioEntity)-[r:indra_rel]->(gene:BioEntity)
        WHERE
            gene.id STARTS WITH "hgnc"                  // Collecting human genes only
            AND r.stmt_type <> "Complex"                // Ignore complexes since they are non-directional
            AND NOT regulator.id STARTS WITH "uniprot"  // This is a simple way to ignore non-human proteins
        RETURN
            regulator.id,
            regulator.name,
            collect([gene.id, r.belief, r.evidence_count]);
    """
    )
    genes_with_confidence = collect_genes_with_confidence(
        client=client, query=query, cache_file=TO_TARGETS_GENE_SETS_PATH
    )
    return filter_gene_set_confidences(
        genes_with_confidence,
        minimum_belief=minimum_belief,
        minimum_evidence_count=minimum_evidence_count,
    )


@autoclient()
def get_entity_to_regulators(
    *,
    client: Neo4jClient,
    minimum_evidence_count: Optional[int] = 1,
    minimum_belief: Optional[float] = 0.0,
) -> Dict[Tuple[str, str], Set[str]]:
    """Get a mapping from each entity in the INDRA database to the set of
    human genes that are causally upstream of it.

    Parameters
    ----------
    client :
        The Neo4j client.
    minimum_evidence_count :
        The minimum number of evidences for a relationship to count it as a regulator.
        Defaults to 1 (i.e., cutoff not applied.
    minimum_belief :
        The minimum belief for a relationship to count it as a regulator.
        Defaults to 0.0 (i.e., cutoff not applied).

    Returns
    -------
    :
        A dictionary whose keys that are 2-tuples of CURIE and name of each entity
        and whose values are sets of HGNC gene identifiers (as strings)
    """
    query = dedent(
        f"""\
        MATCH (gene:BioEntity)-[r:indra_rel]->(target:BioEntity)
        WHERE
            gene.id STARTS WITH "hgnc"               // Collecting human genes only
            AND r.stmt_type <> "Complex"             // Ignore complexes since they are non-directional
            AND NOT target.id STARTS WITH "uniprot"  // This is a simple way to ignore non-human proteins
        RETURN
            target.id,
            target.name,
            collect([gene.id, r.belief, r.evidence_count]);
    """
    )
    genes_with_confidence = collect_genes_with_confidence(
        client=client, query=query, cache_file=TO_REGULATORS_GENE_SETS_PATH
    )
    return filter_gene_set_confidences(
        genes_with_confidence,
        minimum_belief=minimum_belief,
        minimum_evidence_count=minimum_evidence_count,
    )


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
    return dedent(
        f"""\
        MATCH (regulator:BioEntity)-[r:indra_rel]->(gene:BioEntity)
        WHERE gene.id STARTS WITH "hgnc"                // Collecting human genes only
            AND r.stmt_type in [{query_range}]          // Ignore complexes since they are non-directional
            AND NOT regulator.id STARTS WITH "uniprot"  // This is a simple way to ignore non-human proteins
            {evidence_line}
            {belief_line}
        RETURN 
            regulator.id, 
            regulator.name, 
            collect([gene.id, r.belief, r.evidence_count]);
    """
    )


@autoclient()
def get_positive_stmt_sets(
    *,
    client: Neo4jClient,
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
    minimum_evidence_count :
        The minimum number of evidences for a relationship.
        Defaults to 1 (i.e., cutoff not applied.
    minimum_belief :
        The minimum belief for a relationship.
        Defaults to 0.0 (i.e., cutoff not applied).

    Returns
    -------
    :
        A dictionary whose keys that are 2-tuples of CURIE and name of each entity
        and whose values are sets of HGNC gene identifiers (as strings)
    """
    return filter_gene_set_confidences(
        collect_genes_with_confidence(
            query=_query(POSITIVE_STMT_TYPES),
            client=client,
            cache_file=POSITIVES_GENE_SETS_PATH,
        ),
        minimum_belief=minimum_belief,
        minimum_evidence_count=minimum_evidence_count,
    )


@autoclient()
def get_negative_stmt_sets(
    *,
    client: Neo4jClient,
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
    minimum_evidence_count :
        The minimum number of evidences for a relationship.
        Defaults to 1 (i.e., cutoff not applied.
    minimum_belief :
        The minimum belief for a relationship.
        Defaults to 0.0 (i.e., cutoff not applied).

    Returns
    -------
    :
        A dictionary whose keys that are 2-tuples of CURIE and name of each entity
        and whose values are sets of HGNC gene identifiers (as strings)
    """
    return filter_gene_set_confidences(
        collect_genes_with_confidence(
            query=_query(NEGATIVE_STMT_TYPES),
            client=client,
            cache_file=NEGATIVES_GENE_SETS_PATH,
        ),
        minimum_belief=minimum_belief,
        minimum_evidence_count=minimum_evidence_count,
    )


def build_caches():
    """Call each gene set constuction to build up cache,"""
    logger.info("Building up caches for gene set enrichment analysis...")
    get_go()
    get_reactome()
    get_wikipathways()
    get_entity_to_targets(minimum_evidence_count=1, minimum_belief=0.0)
    get_entity_to_regulators(minimum_evidence_count=1, minimum_belief=0.0)
    get_negative_stmt_sets()
    get_positive_stmt_sets()
    logger.info("Finished building caches for gene set enrichment analysis.")
