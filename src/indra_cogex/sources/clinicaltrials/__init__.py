import gilda
import pandas as pd
import tqdm
from indra_cogex.sources.processor import Processor
from indra_cogex.representation import Node, Relation


class ClinicaltrialsProcessor(Processor):
    name = "clinicaltrials"

    def __init__(self, path):
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
            for condition in str(row["Condition"]).split("|"):
                cond_term = self.ground_condition(condition)
                if cond_term:
                    cond_ns = cond_term.db
                    cond_id = cond_term.id
                else:
                    cond_ns = "MESH"
                    cond_id = row["ConditionMeshId"]
                self.has_trial_cond_ns.append(cond_ns)
                self.has_trial_cond_id.append(cond_id)
                self.has_trial_nct.append(row["NCTId"])
                yield Node(db_ns=cond_ns, db_id=cond_id, labels=["BioEntity"])

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

            yield Node(db_ns="CLINICALTRIALS", db_id=row["NCTId"], labels=[])

    def get_relations(self):
        for cond_ns, cond_id, target_id in zip(
            self.has_trial_cond_ns, self.has_trial_cond_id, self.has_trial_nct
        ):
            yield Relation(
                source_ns=cond_ns,
                source_id=cond_id,
                target_ns="CLINICALTRIALS",
                target_id=target_id,
                rel_type="has_trial",
            )
        for int_ns, int_id, target_id in zip(
            self.tested_in_int_ns, self.tested_in_int_id, self.tested_in_nct
        ):
            yield Relation(
                source_ns=int_ns,
                source_id=int_id,
                target_ns="CLINICALTRIALS",
                target_id=target_id,
                rel_type="tested_in",
            )
