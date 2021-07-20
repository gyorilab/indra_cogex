import re
import pandas as pd
import gilda
from indra_cogex.sources.processor import Processor
from indra_cogex.representation import Node, Relation

drug_pattern = re.compile(r"^Drug: (.+)$")


class ClinicaltrialsProcessor(Processor):

    name = "clinicaltrials"

    def __init__(self, path):
        self.df = pd.read_csv(path, sep="\t")
        self.has_trial_cond = []
        self.has_trial_nct = []
        self.tested_in_int = []
        self.tested_in_nct = []

    def get_nodes(self):
        for index, row in self.df.iterrows():
            valid_cond = False
            valid_int = False

            for condition in row["Conditions"].split("|"):
                cond_matches = gilda.ground(condition)
                if cond_matches:
                    valid_cond = True
                    yield Node(
                        db_ns=cond_matches[0].term.db,
                        db_id=cond_matches[0].term.id,
                        labels=[],
                    )

            if not pd.isna(row["Interventions"]):
                for intervention in row["Interventions"].split("|"):
                    if drug_pattern.match(intervention):
                        int_matches = gilda.ground(intervention[6:])
                        if int_matches:
                            valid_int = True
                            yield Node(
                                db_ns=int_matches[0].term.db,
                                db_id=int_matches[0].term.id,
                                labels=[],
                            )

            if valid_cond:
                self.has_trial_cond.append(
                    (cond_matches[0].term.db, cond_matches[0].term.id)
                )
                self.has_trial_nct.append((row["URL"][32:]))

            if valid_int:
                self.tested_in_int.append(
                    (int_matches[0].term.db, int_matches[0].term.id)
                )
                self.tested_in_nct.append((row["URL"][32:]))

            if valid_cond or valid_int:
                yield Node(db_ns="CLINICALTRIALS", db_id=row["URL"][32:], labels=[])

    def get_relations(self):
        for (cond_ns, cond_id), target_id in zip(
            self.has_trial_cond, self.has_trial_nct
        ):
            yield Relation(
                source_ns=cond_ns,
                source_id=cond_id,
                target_ns="CLINICALTRIALS",
                target_id=target_id,
                rel_type="has_trial",
            )
        for (int_ns, int_id), target_id in zip(self.tested_in_int, self.tested_in_nct):
            yield Relation(
                source_ns=int_ns,
                source_id=int_id,
                target_ns="CLINICALTRIALS",
                target_id=target_id,
                rel_type="tested_in",
            )
