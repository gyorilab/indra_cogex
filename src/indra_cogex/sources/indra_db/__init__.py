# -*- coding: utf-8 -*-

"""Processor for the INDRA database."""

import json
import logging
import pickle
from pathlib import Path
from tqdm import tqdm
from typing import Tuple, Union

import humanize
import pandas as pd
import pystow

from indra.ontology.bio import bio_ontology
from indra.databases.identifiers import ensure_prefix_if_needed
from indra_cogex.representation import Node, Relation
from indra_cogex.sources.processor import Processor

logger = logging.getLogger(__name__)
tqdm.pandas()


# If you don't have the data, get it from:
# 's3://bigmech/indra-db/dumps/2021-01-26/sif.pkl'


class DbProcessor(Processor):
    """Processor for the INDRA database."""

    name = "database"
    df: pd.DataFrame

    def __init__(self, path: Union[None, str, Path] = None):
        """Initialize the INDRA database processor.

        :param path: The path to the INDRA database SIF dump pickle. If none given, will look in the default location.
        """
        if path is None:
            path = pystow.join("indra", "db", name="sif.pkl")
        elif isinstance(path, str):
            path = Path(path)
        with open(path, "rb") as fh:
            logger.info("Loading %s" % path)
            df = pickle.load(fh)
        logger.info("Loaded %s rows from %s", humanize.intword(len(df)), path)
        self.df = df
        logger.info("Fixing ID and naming issues...")
        for side in "AB":
            # A lot of the names in the SIF dump are all over
            self.df[f"ag{side}_name"] = [
                bio_ontology.get_name(prefix, identifier)
                for prefix, identifier in self.df[
                    [f"ag{side}_ns", f"ag{side}_id"]
                ].values
            ]
            self.df[f"ag{side}_ns"], self.df[f"ag{side}_id"] = list(
                zip(
                    *[
                        fix_id(db_ns, db_id)
                        for db_ns, db_id in tqdm(
                            zip(list(df[f"ag{side}_ns"]), list(df[f"ag{side}_id"])),
                            total=len(df),
                            desc="Fixing IDs",
                        )
                    ]
                )
            )
        self.df["source_counts"] = self.df["source_counts"].apply(json.dumps)

    def get_nodes(self):  # noqa:D102
        df = pd.concat(
            [
                self.df[["agA_ns", "agA_id", "agA_name"]].rename(
                    columns={"agA_ns": "ns", "agA_id": "id", "agA_name": "name"}
                ),
                self.df[["agB_ns", "agB_id", "agB_name"]].rename(
                    columns={"agB_ns": "ns", "agB_id": "id", "agB_name": "name"}
                ),
            ],
            ignore_index=True,
        ).drop_duplicates()
        for db_ns, db_id, name in df.values:
            yield Node(db_ns, db_id, ["BioEntity"], dict(name=name))

    def get_relations(self):  # noqa:D102
        columns = [
            "agA_ns",
            "agA_id",
            "agB_ns",
            "agB_id",
            "stmt_type",
            "source_counts",
            "stmt_hash",
        ]
        for (
            source_ns,
            source_id,
            target_ns,
            target_id,
            stmt_type,
            source_counts,
            stmt_hash,
        ) in (
            self.df[columns].drop_duplicates().values
        ):
            data = {"stmt_hash:long": stmt_hash, "source_counts:string": source_counts}
            yield Relation(
                source_ns,
                source_id,
                target_ns,
                target_id,
                stmt_type,
                data,
            )


def fix_id(db_ns: str, db_id: str) -> Tuple[str, str]:
    """Fix ID issues specific to the SIF dump."""
    if db_ns == "GO":
        if db_id.isnumeric():
            db_id = "0" * (7 - len(db_id)) + db_id
    if db_ns == "EFO" and db_id.startswith("EFO:"):
        db_id = db_id[4:]
    if db_ns == "UP" and db_id.startswith("SL"):
        db_ns = "UPLOC"
    if db_ns == "UP" and "-" in db_id and not db_id.startswith("SL-"):
        db_id = db_id.split("-")[0]
    db_id = ensure_prefix_if_needed(db_ns, db_id)
    return db_ns, db_id
