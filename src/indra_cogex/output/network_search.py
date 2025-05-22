import json
import logging
from typing import Optional

import pandas as pd
from tqdm import tqdm

from indra_cogex.client import Neo4jClient


logger = logging.getLogger(__name__)


def from_cogex(client: Neo4jClient, limit: Optional[int] = None) -> pd.DataFrame:
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
