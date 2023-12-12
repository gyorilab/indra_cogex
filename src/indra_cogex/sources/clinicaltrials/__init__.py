"""This module implements input for ClinicalTrials.gov data."""

import logging
from collections import Counter
from pathlib import Path
from typing import Union

import gilda
import pandas as pd
import tqdm

from indra.databases import mesh_client
from indra_cogex.sources.processor import Processor
from indra_cogex.representation import Node, Relation
from indra_cogex.sources.clinicaltrials.download import ensure_clinical_trials_df


logger = logging.getLogger(__name__)


class ClinicaltrialsProcessor(Processor):
    name = "clinicaltrials"
    node_types = ["BioEntity", "ClinicalTrial"]

    def __init__(self, path: Union[str, Path, None] = None):
        if path is not None:
            self.df = pd.read_csv(path, sep=",", skiprows=10)
        else:
            self.df = ensure_clinical_trials_df()
        process_df(self.df)

        self.has_trial_cond_ns = []
        self.has_trial_cond_id = []
        self.has_trial_nct = []
        self.tested_in_int_ns = []
        self.tested_in_int_id = []
        self.tested_in_nct = []

        self.problematic_mesh_ids = []

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
        nctid_to_data = {}
        for _, row in tqdm.tqdm(self.df.iterrows(), total=len(self.df)):
            nctid_to_data[row["NCTId"]] = {
                "study_type": or_na(row["StudyType"]),  # observational, interventional
                "randomized:boolean": row["randomized"],
                "status": or_na(row["OverallStatus"]),  # Completed, Active, Recruiting
                "phase:int": row["Phase"],
                "why_stopped": or_na(row["WhyStopped"]),
                "start_year:int": row["start_year"],
                "start_year_anticipated:boolean": row["start_year_anticipated"],
            }

            found_disease_gilda = False
            for condition in str(row["Condition"]).split("|"):
                cond_term = self.ground_condition(condition)
                if cond_term:
                    self.has_trial_nct.append(row["NCTId"])
                    self.has_trial_cond_ns.append(cond_term.db)
                    self.has_trial_cond_id.append(cond_term.id)
                    yield Node(
                        db_ns=cond_term.db, db_id=cond_term.id, labels=["BioEntity"]
                    )
                    found_disease_gilda = True
            if not found_disease_gilda and not pd.isna(row["ConditionMeshId"]):
                for mesh_id, mesh_term in zip(row["ConditionMeshId"].split("|"),
                                              row["ConditionMeshTerm"].split("|")):
                    correct_mesh_id = get_correct_mesh_id(mesh_id, mesh_term)
                    if not correct_mesh_id:
                        self.problematic_mesh_ids.append((mesh_id, mesh_term))
                        continue
                    self.has_trial_nct.append(row["NCTId"])
                    self.has_trial_cond_ns.append("MESH")
                    self.has_trial_cond_id.append(correct_mesh_id)
                    yield Node(db_ns="MESH", db_id=correct_mesh_id, labels=["BioEntity"])

            # We first try grounding the names with Gilda, if any match, we
            # use it, if there are no matches, we go by provided MeSH ID
            found_drug_gilda = False
            for int_name, int_type in zip(
                str(row["InterventionName"]).split("|"),
                str(row["InterventionType"]).split("|"),
            ):
                if int_type == "Drug":
                    drug_term = self.ground_drug(int_name)
                    if drug_term:
                        self.tested_in_int_ns.append(drug_term.db)
                        self.tested_in_int_id.append(drug_term.id)
                        self.tested_in_nct.append(row["NCTId"])
                        yield Node(
                            db_ns=drug_term.db, db_id=drug_term.id, labels=["BioEntity"]
                        )
                        found_drug_gilda = True
            # If there is no Gilda much but there are some MeSH IDs given
            if not found_drug_gilda and not pd.isna(row["InterventionMeshId"]):
                for mesh_id, mesh_term in zip(row["InterventionMeshId"].split("|"),
                                              row["InterventionMeshTerm"].split("|")):
                    correct_mesh_id = get_correct_mesh_id(mesh_id, mesh_term)
                    if not correct_mesh_id:
                        self.problematic_mesh_ids.append((mesh_id, mesh_term))
                        continue
                    self.tested_in_int_ns.append("MESH")
                    self.tested_in_int_id.append(correct_mesh_id)
                    self.tested_in_nct.append(row["NCTId"])
                    yield Node(db_ns="MESH", db_id=correct_mesh_id, labels=["BioEntity"])

        for nctid in set(self.tested_in_nct) | set(self.has_trial_nct):
            yield Node(
                db_ns="CLINICALTRIALS",
                db_id=nctid,
                labels=["ClinicalTrial"],
                data=nctid_to_data[nctid],
            )

        logger.info('Problematic MeSH IDs: %s' % str(
            Counter(self.problematic_mesh_ids).most_common()))

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
    if mesh_client.get_mesh_name(mesh_id, offline=True):
        return mesh_id
    # A common issue is with zero padding, where 9 digits are used
    # instead of the correct 6, and we can remove the extra zeros
    # to get a valid ID
    else:
        short_id = mesh_id[0] + mesh_id[4:]
        if mesh_client.get_mesh_name(short_id, offline=True):
            return short_id
    # Another pattern is one where the MeSH ID is simply invalid but the
    # corresponding MeSH term allows us to get a valid ID via reverse
    # ID lookup - done here as grounding just to not have to assume
    # perfect / up to date naming conventions in the source data.
    if mesh_term:
        matches = gilda.ground(mesh_term, namespaces=['MESH'])
        if len(matches) == 1:
            for k, v in matches[0].get_groundings():
                if k == 'MESH':
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
