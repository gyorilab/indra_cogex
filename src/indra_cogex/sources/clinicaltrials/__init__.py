"""This module implements input for ClinicalTrials.gov data."""

import logging

import pandas as pd
import tqdm

from indra.ontology.bio import bio_ontology
from indra_cogex.client import process_identifier
from indra_cogex.sources.processor import Processor
from indra_cogex.representation import Node, Relation
from indra_cogex.sources.utils import get_bool
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

    def __init__(self):
        ensure_clinical_trials_df()

        self.trials_df = process_trialsynth_trial_nodes()
        # Warm up bio ontology
        _ = bio_ontology.get_name("HGNC", "1100")
        self.edges_df = process_trialsynth_edges()
        self.mesh_chebi_map = {
            old_id: new_id for new_id, old_id in
            self.edges_df[["bioentity_mapped", "bioentity"]].values
            if new_id.lower().startswith("chebi:") and old_id.lower().startswith("mesh:")
        }
        self.bioentities_df = process_trialsynth_bioentity_nodes(self.mesh_chebi_map)

    def get_nodes(self):
        yielded_nodes = set()
        for ix, row in tqdm.tqdm(
            self.trials_df.iterrows(), total=len(self.trials_df), desc="Trial nodes"
        ):
            nctid = row["id:ID"]
            if nctid in yielded_nodes:
                continue
            yielded_nodes.add(nctid)
            db_ns, db_id = process_identifier(nctid)
            yield Node(
                db_ns=db_ns,
                db_id=db_id,
                labels=["ClinicalTrial"],
                data={
                    "study_type": or_na(row["study_type"]),
                    "randomized:boolean": get_bool(row["randomized:boolean"]),
                    "status": or_na(row["status"]),
                    "phase:int": row["phase:int"],
                    "why_stopped": or_na(row["why_stopped"]),
                    "start_year:int": or_na(row["start_year"]),
                    "start_year_anticipated:boolean": get_bool(
                        row["start_year_anticipated:boolean"]
                    ),
                },
            )

        for ix, row in tqdm.tqdm(
            self.bioentities_df.iterrows(), total=len(self.bioentities_df), desc="BioEntity nodes"
        ):
            bioentity = row["bioentity_mapped"]
            if bioentity in yielded_nodes:
                continue
            yielded_nodes.add(bioentity)
            db_ns, db_id = process_identifier(bioentity)
            yield Node(
                db_ns=db_ns,
                db_id=db_id,
                labels=["BioEntity"],
                data={"name": row["name"]},
            )

    def get_relations(self):
        added = set()
        rel_translation = {
            "has_condition": "has_trial",
            "has_intervention": "tested_in",
        }
        for ix, row in tqdm.tqdm(
            self.edges_df.iterrows(), total=len(self.edges_df), desc="Edges"
        ):
            # Conditions: use "has_trial" relation going to the trial from the condition
            # Interventions: use "tested_in" relation going to the trial from the intervention
            # The Trialsynth edges go from the trial to the bioentity with a
            # has_intervention or has_condition relation. In CoGEx the edge goes
            # from the bioentity to the trial with a tested_in or has_trial edge

            bioentity = row["bioentity_mapped"]
            rel_type = rel_translation.get(row["rel_type:string"])
            if rel_type is None:
                raise ValueError(f"Unknown relation type: {row['rel_type:string']}")

            nctid_curie = row["trial"]
            if (bioentity, nctid_curie, rel_type) in added:
                continue

            db_ns, db_id = process_identifier(bioentity)
            trial_ns, trial_id = process_identifier(nctid_curie)
            yield Relation(
                source_ns=db_ns,
                source_id=db_id,
                target_ns=trial_ns,
                target_id=trial_id,
                rel_type=rel_type,
            )
            added.add((bioentity, nctid_curie, rel_type))


def or_na(x):
    """Return None if x is NaN, otherwise return x"""
    return None if pd.isna(x) else x
