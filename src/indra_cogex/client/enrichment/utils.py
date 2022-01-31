# -*- coding: utf-8 -*-

"""Utilities for getting gene sets."""

import logging
from collections import defaultdict
from functools import lru_cache
from textwrap import dedent
from typing import Dict, Set, Tuple

from indra_cogex.client.neo4j_client import Neo4jClient, autoclient

__all__ = [
    "collect_gene_sets",
    "get_go",
    "get_wikipathways",
    "get_reactome",
    "get_entity_to_targets",
    "get_entity_to_regulators",
]

logger = logging.getLogger(__name__)


@autoclient()
def collect_gene_sets(
    query: str,
    *,
    client: Neo4jClient,
) -> Dict[Tuple[str, str], Set[str]]:
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
        item and whose values are sets of HGNC gene identifiers (as strings)
    """
    curie_to_hgnc_ids = defaultdict(set)
    for result in client.query_tx(query):
        curie = result[0]
        name = result[1]
        hgnc_ids = {
            hgnc_curie.lower().replace("hgnc:", "")
            if hgnc_curie.lower().startswith("hgnc:")
            else hgnc_curie.lower()
            for hgnc_curie in result[2]
        }
        curie_to_hgnc_ids[curie, name].update(hgnc_ids)
    return dict(curie_to_hgnc_ids)


@autoclient(cache=True)
@lru_cache(maxsize=1)
def get_go(*, client: Neo4jClient) -> Dict[Tuple[str, str], Set[str]]:
    """Get GO gene sets.

    Parameters
    ----------
    client :object
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
    logger.info("caching GO with Cypher query: %s", query)
    return collect_gene_sets(client=client, query=query)


@autoclient(cache=True)
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
    logger.info("caching WikiPathways with Cypher query: %s", query)
    return collect_gene_sets(client=client, query=query)


@autoclient(cache=True)
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
    logger.info("caching Reactome with Cypher query: %s", query)
    return collect_gene_sets(client=client, query=query)


@autoclient(cache=True)
def get_entity_to_targets(*, client: Neo4jClient) -> Dict[Tuple[str, str], Set[str]]:
    """Get a mapping from each entity in the INDRA database to the set of
    human genes that it regulates.

    Parameters
    ----------
    client :
        The Neo4j client.

    Returns
    -------
    :
        A dictionary whose keys that are 2-tuples of CURIE and name of each entity
        and whose values are sets of HGNC gene identifiers (as strings)
    """
    query = dedent(
        """\
        MATCH (regulator:BioEntity)-[r:indra_rel]->(gene:BioEntity)
        // Collecting human genes only
        WHERE gene.id STARTS WITH "hgnc"
        // Ignore complexes since they are non-directional
        AND r.stmt_type <> "Complex"
        // This is a simple way to ignore non-human proteins
        AND NOT regulator.id STARTS WITH "uniprot"
        RETURN regulator.id, regulator.name, collect(gene.id);
    """
    )
    logger.info("caching entity->targets with Cypher query: %s", query)
    return collect_gene_sets(client=client, query=query)


@autoclient(cache=True)
def get_entity_to_regulators(*, client: Neo4jClient) -> Dict[Tuple[str, str], Set[str]]:
    """Get a mapping from each entity in the INDRA database to the set of
    human genes that are causally upstream of it.

    Parameters
    ----------
    client :
        The Neo4j client.

    Returns
    -------
    :
        A dictionary whose keys that are 2-tuples of CURIE and name of each entity
        and whose values are sets of HGNC gene identifiers (as strings)
    """
    query = dedent(
        """\
        MATCH (gene:BioEntity)-[r:indra_rel]->(target:BioEntity)
        // Collecting human genes only
        WHERE gene.id STARTS WITH "hgnc"
        // Ignore complexes since they are non-directional
        AND r.stmt_type <> "Complex"
        // This is a simple way to ignore non-human proteins
        AND NOT target.id STARTS WITH "uniprot"
        RETURN target.id, target.name, collect(gene.id);
    """
    )
    logger.info("caching entity->regulators with Cypher query: %s", query)
    return collect_gene_sets(client=client, query=query)
