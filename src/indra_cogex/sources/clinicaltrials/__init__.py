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

import gilda
import pandas as pd
from pathlib import Path
import pystow
import tqdm
from typing import Union
from indra_cogex.sources.processor import Processor
from indra_cogex.representation import Node, Relation


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
                for mesh_id in row["ConditionMeshId"].split("|"):
                    self.has_trial_nct.append(row["NCTId"])
                    self.has_trial_cond_ns.append("MESH")
                    self.has_trial_cond_id.append(mesh_id)
                    yield Node(db_ns="MESH", db_id=mesh_id, labels=["BioEntity"])

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
                for mesh_id in row["InterventionMeshId"].split("|"):
                    self.tested_in_int_ns.append("MESH")
                    self.tested_in_int_id.append(mesh_id)
                    self.tested_in_nct.append(row["NCTId"])
                    yield Node(db_ns="MESH", db_id=mesh_id, labels=["BioEntity"])

        for nctid in set(self.tested_in_nct) | set(self.has_trial_nct):
            yield Node(db_ns="CLINICALTRIALS", db_id=nctid, labels=["ClinicalTrial"])

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
