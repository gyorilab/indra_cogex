import gilda
import pandas as pd
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

    def get_nodes(self):
        for index, row in self.df.iterrows():
            for condition in str(row["Condition"]).split("|"):
                cond_matches = gilda.ground(condition)
                if cond_matches:
                    self.has_trial_cond_ns.append(cond_matches[0].term.db)
                    self.has_trial_cond_id.append(cond_matches[0].term.id)
                    self.has_trial_nct.append(row["NCTId"])
                    yield Node(
                        db_ns=cond_matches[0].term.db,
                        db_id=cond_matches[0].term.id,
                        labels=[],
                    )
                else:
                    self.has_trial_cond_ns.append("MESH")
                    self.has_trial_cond_id.append(row["ConditionMeshId"])
                    self.has_trial_nct.append(row["NCTId"])
                    yield Node(
                        db_ns="MESH",
                        db_id=row["ConditionMeshId"],
                        labels=[],
                    )

            for int_name, int_type in zip(
                str(row["InterventionName"]).split("|"),
                str(row["InterventionType"]).split("|"),
            ):
                if int_type == "Drug":
                    int_matches = gilda.ground(int_name)
                    if int_matches:
                        self.tested_in_int_ns.append(int_matches[0].term.db)
                        self.tested_in_int_id.append(int_matches[0].term.id)
                        self.tested_in_nct.append(row["NCTId"])
                        yield Node(
                            db_ns=int_matches[0].term.db,
                            db_id=int_matches[0].term.id,
                            labels=[],
                        )
                    else:
                        self.tested_in_int_ns.append("MESH")
                        self.tested_in_int_id.append(row["InterventionMeshTerm"])
                        self.tested_in_nct.append(row["NCTId"])
                        yield Node(
                            db_ns="MESH",
                            db_id=row["InterventionMeshTerm"],
                            labels=[],
                        )

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
