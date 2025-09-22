import json
import logging
from typing import Optional, Tuple, List

import pandas as pd
from tqdm import tqdm

from indra_cogex.client import Neo4jClient


logger = logging.getLogger(__name__)


def sif_with_logp(
    client: Neo4jClient,
    limit: Optional[int] = None,
    batch_size: int = 250_000
) -> pd.DataFrame:
    """Get a dataframe with all indra_rel relations and their logp values.

    Parameters
    ----------
    client :
        A Neo4jClient instance to query the database.
    limit :
        An optional limit on the number of relations to return.
        If None, no limit is applied. The limit is applied to both
        indra_rel and codependent_with relations. Use to test on a
        smaller subset of the data.
    batch_size :
        The number of relations to fetch in each batch.
        Defaults to 250,000. If the limit is set, this will be adjusted to the
        limit.
    Returns
    -------
    :
        A pandas DataFrame with the following columns:
        - agA_ns: Namespace of the first agent
        - agA_id: ID of the first agent
        - agA_name: Name of the first agent
        - agB_ns: Namespace of the second agent
        - agB_id: ID of the second agent
        - agB_name: Name of the second agent
        - stmt_type: Type of the statement (e.g., 'phosphorylation')
        - evidence_count: Number of pieces of evidence supporting the statement
        - stmt_hash: Hash of the statement
        - residue: Residue involved in the statement (if applicable)
        - position: Position of the residue (if applicable)
        - source_counts: A dictionary with counts of sources for the statement
        - belief: Belief score of the statement
        - logp: Logarithm of the p-value for the codependent_with relation
    """
    if limit and limit < batch_size:
        batch_size = limit
    # First get all indra_rel relations

    # Count how many relations there are in total
    if limit is None:
        count_query = """\
        MATCH (:BioEntity)-[rel:indra_rel]->(:BioEntity)
        RETURN count(DISTINCT rel) AS count"""
        count = client.query_tx(count_query)[0][0]
        logger.info(f"Found {count} indra_rel relations")
        total_rels = count
    else:
        logger.info(f"Limiting to {limit} indra_rel relations")
        total_rels = limit

    indra_rel_query = """\
    MATCH p=(source:BioEntity)-[rel:indra_rel]->(target:BioEntity)
    RETURN DISTINCT p
    SKIP $skip
    LIMIT $limit
    """

    # Batched retrieval of statement relations
    logger.info("Getting indra_rel relations")
    stmt_rows = []
    offset = 0
    remaining = limit
    with tqdm(
        total=total_rels,
        desc="Creating indra_rel dataframe",
        unit="relation",
        unit_scale=True,
    ) as pbar:
        while True:
            current_limit = batch_size if remaining is None else min(batch_size, remaining)
            results = client.query_relations(
                indra_rel_query,
                skip=offset,
                limit=current_limit
            )
            if not results:
                break
            for rel in results:
                stmt_json = json.loads(rel.data["stmt_json"])
                source_count = json.loads(rel.data["source_counts"])
                stmt_rows.append(
                    (
                        rel.source_ns,
                        rel.source_id,
                        rel.source_name,
                        rel.target_ns,
                        rel.target_id,
                        rel.target_name,
                        rel.data["stmt_type"],
                        rel.data["evidence_count"],  # int
                        rel.data["stmt_hash"],  # int
                        stmt_json.get("residue"),
                        stmt_json.get("position"),  # int
                        source_count,  # dict[str, int], sum() == evidence_count
                        rel.data["belief"],  # float
                    )
                )
            # Update progress bar and offset
            new_rels = len(results)
            offset += new_rels
            pbar.update(new_rels)
            if remaining is not None:
                remaining -= new_rels
                if remaining <= 0:
                    break
            # If we got fewer results than requested, we are done
            if new_rels < current_limit:
                break

    # Create the dataframe
    sif_df = pd.DataFrame(
        stmt_rows, columns=[
            "agA_ns",
            "agA_id",
            "agA_name",
            "agB_ns",
            "agB_id",
            "agB_name",
            "stmt_type",
            "evidence_count",
            "stmt_hash",
            "residue",
            "position",
            "source_counts",
            "belief",
        ]
    ).astype(
        {
            "position": "Int64",  # nullable int
            "evidence_count": "int64",  # int
            "stmt_hash": "int64",  # int
            "belief": "float64",  # float
        }
    )

    # Count the total number of codependent_with relations
    if limit is None:
        z_count_query = """\
        MATCH (:BioEntity)-[rel:codependent_with]->(:BioEntity)
        RETURN count(DISTINCT rel) AS count"""
        total_z = client.query_tx(z_count_query)[0][0]
        logger.info(f"Found {total_z} codependent_with relations")
    else:
        logger.info(f"Limiting to {limit} codependent_with relations")
        total_z = limit

    # Then get all the codependent_with relations
    z_score_query = """\
    MATCH p=(source:BioEntity)-[rel:codependent_with]->(target:BioEntity)
    RETURN DISTINCT p
    SKIP $skip
    LIMIT $limit
    """
    logger.info("Getting codependent_with relations")
    z_score_rows = []
    offset = 0
    remaining = limit
    with tqdm(
        total=total_z,
        desc="codependent_with batches",
        unit="relation",
        unit_scale=True,
    ) as pbar:
        while True:
            current_limit = batch_size if remaining is None else min(batch_size, remaining)
            results = client.query_relations(
                z_score_query,
                skip=offset,
                limit=current_limit
            )
            if not results:
                break
            for rel in results:
                z_score_rows.append(
                    (
                        rel.source_ns,
                        rel.source_id,
                        rel.source_name,
                        rel.target_ns,
                        rel.target_id,
                        rel.target_name,
                        rel.data["logp"],
                    )
                )
            # Update progress bar and offset
            new_rels = len(results)
            offset += new_rels
            pbar.update(new_rels)
            if remaining is not None:
                remaining -= new_rels
                if remaining <= 0:
                    break
            # If we got fewer results than requested, we are done
            if new_rels < current_limit:
                break

    # Create the dataframe
    z_score_df = pd.DataFrame(
        z_score_rows,
        columns=[
            "agA_ns",
            "agA_id",
            "agA_name",
            "agB_ns",
            "agB_id",
            "agB_name",
            "logp",
        ],
    ).astype(
        {
            "logp": "float64",  # float
        }
    )

    # Merge the two dataframes
    logger.info("Merging dataframes")
    sif_df = pd.merge(
        sif_df,
        z_score_df,
        on=["agA_ns", "agA_id", "agA_name", "agB_ns", "agB_id", "agB_name"],
        # Do left join to keep all indra_rel relations and add logp values where available
        how="left",
    )
    return sif_df


def get_stmt_hash_to_mesh_map(
    client: Neo4jClient,
    batch_size: int = 100_000,
    limit: Optional[int] = None
) -> List[Tuple[str, str]]:
    """Get a dataframe mapping statement hashes to mesh IDs.

    Parameters
    ----------
    client :
        A Neo4jClient instance to query the database.
    batch_size :
        The number of relations to fetch in each batch.
        Defaults to 100,000.
        If the limit is set, this will be adjusted to the limit.
    limit :
        An optional limit on the number of relations to return.
        If None, no limit is applied.

    Returns
    -------
    :
        A pandas DataFrame with the following columns:
        - stmt_hash: Hash of the statement
        - mesh_id: Mesh ID associated with the statement
    """
    query = """\
    MATCH (e:Evidence)-[:has_citation]->(:Publication)-[:annotated_with]->(mid:BioEntity)
    RETURN DISTINCT e.stmt_hash, mid.id
    SKIP $offset
    LIMIT $limit
    """
    if limit and limit < batch_size:
        batch_size = limit

    logger.info("Getting statement hash to mesh ID mapping")
    rows = []
    # Run until we reach the limit or exhaust the relations
    # Run the first query
    with tqdm(
        total=limit if limit else None,
        unit="batch",
        desc="hash-mesh map"
    ) as pbar:
        results = client.query_tx(query, offset=0, limit=batch_size)
        while len(results) == batch_size:
            rows.extend(results)
            pbar.update(len(results))
            if limit and len(rows) >= limit:
                break
            results = client.query_tx(query, offset=len(rows), limit=batch_size)

    return rows
