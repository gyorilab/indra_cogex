"""This module implements input for ClinicalTrials.gov data

To obtain the custom download for ingest, do the following

1. Go to https://clinicaltrials.gov/api/gui/demo/simple_study_fields

2. Enter the following in the form:

expr=
fields=NCTId,BriefTitle,Condition,ConditionMeshTerm,ConditionMeshId,InterventionName,InterventionType,InterventionMeshTerm,InterventionMeshId
min_rnk=1
max_rnk=500000  # or any number larger than the current number of studies
fmt=csv

3. Send Request

4. Enter the captcha characters into the text box and then press enter
(make sure to use the enter key and not press any buttons).

5. The website will display "please wait… " for a couple of minutes, finally,
the Save to file button will be active.

6. Click the Save to file button to download the response as a txt file.

7. Rename the txt file to clinical_trials.csv and then compress it as
gzip clinical_trials.csv to get clinical_trials.csv.gz, then place
this file into <pystow home>/indra/cogex/clinicaltrials/
"""

import logging
from collections import Counter
from pathlib import Path
from typing import Union

import gilda
import pandas as pd
import pystow
import tqdm

from indra.databases import mesh_client
from indra_cogex.sources.processor import Processor
from indra_cogex.representation import Node, Relation


logger = logging.getLogger(__name__)


class ClinicaltrialsProcessor(Processor):
    name = "clinicaltrials"
    node_types = ["BioEntity", "ClinicalTrial"]

    def __init__(self, path: Union[str, Path, None] = None):
        default_path = pystow.join(
            "indra",
            "cogex",
            "clinicaltrials",
            name="clinical_trials.csv.gz",
        )

        if not path:
            path = default_path
        elif isinstance(path, str):
            path = Path(path)

        self.df = pd.read_csv(path, sep=",", skiprows=10)
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
        for index, row in tqdm.tqdm(self.df.iterrows(), total=len(self.df)):
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
            yield Node(db_ns="CLINICALTRIALS", db_id=nctid, labels=["ClinicalTrial"])

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
