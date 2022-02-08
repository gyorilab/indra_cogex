# -*- coding: utf-8 -*-

"""Processor for ChEMBL."""

import logging
from typing import Iterable, Optional

import bioversions
import chembl_downloader
from tqdm import tqdm

from indra_cogex.representation import Node, Relation
from indra_cogex.sources.processor import Processor

logger = logging.getLogger(__name__)

#: SQL for ChEMBL to get molecules that have indications
MOLECULE_SQL = f"""
SELECT DISTINCT
    MOLECULE_DICTIONARY.chembl_id,
    MOLECULE_DICTIONARY.pref_name
FROM MOLECULE_DICTIONARY
JOIN DRUG_INDICATION ON MOLECULE_DICTIONARY.molregno == DRUG_INDICATION.molregno
"""

#: SQL for ChEMBL to get indications
SQL = f"""
SELECT
    MOLECULE_DICTIONARY.chembl_id,
    DRUG_INDICATION.mesh_id,
    DRUG_INDICATION.max_phase_for_ind
FROM MOLECULE_DICTIONARY
JOIN DRUG_INDICATION ON MOLECULE_DICTIONARY.molregno == DRUG_INDICATION.molregno
"""


class ChemblIndicationsProcessor(Processor):
    """A processor for ChEMBL indications."""

    name = "chembl"
    node_types = ["BioEntity"]

    def __init__(self, version: Optional[str] = None):
        self.version = version or bioversions.get_version("chembl")
        self.df = chembl_downloader.query(SQL, version=self.version)
        chemical_df = chembl_downloader.query(MOLECULE_SQL, version=version)
        self.chemicals = {
            chembl_id: Node.standardized(
                db_ns="CHEMBL",
                db_id=chembl_id,
                name=chembl_name,
                labels=["BioEntity"],
            )
            for chembl_id, chembl_name in tqdm(
                chemical_df.values, unit_scale=True, desc="caching chemicals"
            )
        }
        self.indications = {
            mesh_id: Node.standardized(
                db_ns="MESH", db_id=mesh_id, labels=["BioEntity"]
            )
            for mesh_id in tqdm(
                self.df.mesh_id.unique(), unit_scale=True, desc="caching indications"
            )
        }

    def get_nodes(self) -> Iterable[Node]:
        """Iterate over ChEMBL chemicals and indications"""
        yield from self.chemicals.values()
        yield from self.indications.values()

    def get_relations(self) -> Iterable[Relation]:
        """Iterate over ChEMBL indication annotations."""
        for chembl_id, mesh_id, max_phase in self.df.values:
            chemical = self.chemicals[chembl_id]
            indication = self.indications[mesh_id]
            yield Relation(
                chemical.db_ns,
                chemical.db_id,
                indication.db_ns,
                indication.db_id,
                "has_indication",
                {
                    "source": self.name,
                    "max_phase:int": max_phase,
                    "version": self.version,
                },
            )
