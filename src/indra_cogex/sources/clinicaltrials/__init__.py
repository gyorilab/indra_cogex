"""This module implements input for ClinicalTrials.gov data."""

import logging
from collections import Counter
from pathlib import Path
from typing import Union

import gilda
import pandas as pd
import tqdm

from indra.databases import mesh_client
from indra.ontology.bio import bio_ontology
from indra_cogex.sources.processor import Processor
from indra_cogex.representation import Node, Relation
from indra_cogex.sources.clinicaltrials.download import (
    ensure_clinical_trials_df,
    process_trialsynth_edges,
    process_trialsynth_bioentity_nodes,
    process_trialsynth_trial_nodes
)


logger = logging.getLogger(__name__)


class ClinicaltrialsProcessor(Processor):
    name = "clinicaltrials"
    node_types = ["BioEntity", "ClinicalTrial"]

    def __init__(self, path: Union[str, Path, None] = None):
        ensure_clinical_trials_df()

        self.trials_df = process_trialsynth_trial_nodes()
        # Warm up bio ontology
        _ = bio_ontology.get_name("HGNC", "1100")
        self.edges_df = process_trialsynth_edges()
        self.mesh_chebi_map = {
            old_id: new_id for new_id, old_id in
            self.edges_df[["bioentity_mapped", "bioentity"]].values
            if new_id.startswith("chebi:") and old_id.startswith("mesh:")
        }
        self.bioentities_df = process_trialsynth_bioentity_nodes(self.mesh_chebi_map)

    def ground_condition(self, condition):
        matches = gilda.ground(condition)
        matches = [
            match
            for match in matches
            if match.term.db in {"MESH", "DOID", "EFO", "HP", "GO"}
        ]
        if matches:
            return matches[0].term
        return None

    def ground_drug(self, drug):
        matches = gilda.ground(drug)
        if matches:
            return matches[0].term
        return None

    def get_nodes(self):
        yielded_nodes = set()
        for ix, row in tqdm.tqdm(
            self.trials_df.iterrows(), total=len(self.trials_df), desc="Trial nodes"
        ):
            nctid = row["NCTId"]
            if nctid in yielded_nodes:
                continue
            yielded_nodes.add(nctid)
            db_ns, db_id = nctid.split(":")
            yield Node(
                db_ns=db_ns,
                db_id=db_id,
                labels=["ClinicalTrial"],
                data={
                    "study_type": or_na(row["study_type"]),
                    "randomized:boolean": row["randomized:boolean"],
                    "status": or_na(row["status"]),
                    "phase:int": row["phase:int"],
                    "why_stopped": or_na(row["why_stopped"]),
                    "start_year:int": or_na(row["start_year"]),
                    "start_year_anticipated:boolean": row["start_year_anticipated"],
                },
            )

        for ix, row in tqdm.tqdm(
            self.bioentities_df.iterrows(), total=len(self.bioentities_df), desc="BioEntity nodes"
        ):
            bioentity = row["bioentity_mapped"]
            if bioentity in yielded_nodes:
                continue
            yielded_nodes.add(bioentity)
            db_ns, db_id = bioentity.split(":")
            yield Node(
                db_ns=db_ns,
                db_id=db_id,
                labels=["BioEntity"],
                data={"name": row["name"]},
            )

    def get_relations(self):
        added = set()
        for cond_ns, cond_id, target_id in zip(
            self.has_trial_cond_ns, self.has_trial_cond_id, self.has_trial_nct
        ):
            if (cond_ns, cond_id, target_id) in added:
                continue
            added.add((cond_ns, cond_id, target_id))
            yield Relation(
                source_ns=cond_ns,
                source_id=cond_id,
                target_ns="CLINICALTRIALS",
                target_id=target_id,
                rel_type="has_trial",
            )
        added = set()
        for int_ns, int_id, target_id in zip(
            self.tested_in_int_ns, self.tested_in_int_id, self.tested_in_nct
        ):
            if (int_ns, int_id, target_id) in added:
                continue
            added.add((int_ns, int_id, target_id))
            yield Relation(
                source_ns=int_ns,
                source_id=int_id,
                target_ns="CLINICALTRIALS",
                target_id=target_id,
                rel_type="tested_in",
            )


def get_correct_mesh_id(mesh_id, mesh_term=None):
    # A proxy for checking whether something is a valid MeSH term is
    # to look up its name
    name = mesh_client.get_mesh_name(mesh_id, offline=True)
    if name:
        return mesh_id
    # A common issue is with zero padding, where 9 digits are used
    # instead of the correct 6, and we can remove the extra zeros
    # to get a valid ID
    else:
        short_id = mesh_id[0] + mesh_id[4:]
        name = mesh_client.get_mesh_name(short_id, offline=True)
        if name:
            return short_id
    # Another pattern is one where the MeSH ID is simply invalid but the
    # corresponding MeSH term allows us to get a valid ID via reverse
    # ID lookup - done here as grounding just to not have to assume
    # perfect / up to date naming conventions in the source data.
    if mesh_term:
        matches = gilda.ground(mesh_term, namespaces=["MESH"])
        if len(matches) == 1:
            for k, v in matches[0].get_groundings():
                if k == "MESH":
                    return v
    return None


def _get_phase(phase_string: str) -> int:
    if pd.notna(phase_string) and phase_string[-1].isdigit():
        return int(phase_string[-1])
    return -1


def process_df(df: pd.DataFrame):
    """Clean up values in DataFrame"""
    # Create start year column from StartDate
    df["start_year"] = (
        df["StartDate"]
        .map(lambda s: None if pd.isna(s) else int(s[-4:]))
        .astype("Int64")
    )

    # randomized, Non-Randomized
    df["randomized"] = df["DesignAllocation"].map(
        lambda s: "true" if pd.notna(s) and s == "Randomized" else "false"
    )

    # Indicate if the start_year is anticipated or not
    df["start_year_anticipated"] = df["StartDateType"].map(
        lambda s: "true" if pd.notna(s) and s == "Anticipated" else "false"
    )

    # Map the phase info for trial to integer (-1 for unknown)
    df["Phase"] = df["Phase"].apply(_get_phase)

    # Create a Neo4j compatible list of references
    df["ReferencePMID"] = df["ReferencePMID"].map(
        lambda s: ";".join(f"PUBMED:{pubmed_id}" for pubmed_id in s.split("|")),
        na_action="ignore",
    )


def or_na(x):
    """Return None if x is NaN, otherwise return x"""
    return None if pd.isna(x) else x
