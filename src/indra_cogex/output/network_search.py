import json
import logging
from typing import Optional, Tuple, List

import pandas as pd
from tqdm import tqdm

from indra_cogex.client import Neo4jClient


logger = logging.getLogger(__name__)


def sif_with_logp(client: Neo4jClient, limit: Optional[int] = None) -> pd.DataFrame:
    """Get a dataframe with all indra_rel relations and their logp values.

    Parameters
    ----------
    client :
        A Neo4jClient instance to query the database.
    limit :
        An optional limit on the number of relations to return.
        If None, no limit is applied.

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
    # First get all indra_rel relations
    indra_rel_query = """\
    MATCH p=(source:BioEntity)-[rel:indra_rel]->(target:BioEntity)
    RETURN DISTINCT p"""
    if limit is not None:
        indra_rel_query += f"\nLIMIT {int(limit)}"

    logger.info("Getting indra_rel relations")
    stmt_rows = []
    for rel in tqdm(
        client.query_relations(indra_rel_query),
        desc="Creating indra_rel dataframe",
        unit="relation",
        unit_scale=True,
    ):
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

    # Then get all the codependent_with relations
    z_score_query = """\
    MATCH p=(source:BioEntity)-[rel:codependent_with]->(target:BioEntity)
    RETURN DISTINCT p"""
    if limit is not None:
        z_score_query += f"\nLIMIT {int(limit)}"

    logger.info("Getting codependent_with relations")
    z_score_rows = []
    for rel in tqdm(
        client.query_relations(z_score_query),
        desc="Creating codependent_with dataframe",
        unit="relation",
        unit_scale=True,
    ):
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
