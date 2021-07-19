import re
import pandas as pd
import gilda
from indra_cogex.sources.processor import Processor
from indra_cogex.representation import Node, Relation

drug_pattern = re.compile(r"^Drug: (.+)$")
# id_pattern = re.compile(r'^https://ClinicalTrials.gov/show/NCT(\d+)$')


class ClinicaltrialsProcessor(Processor):

    name = "clinicaltrials"

    def __init__(self, path):
        self.df = pd.read_csv(path, sep="\t")

    def get_nodes(self):
        for index, row in self.df.iterrows():
            valid = False
            for condition in row["Conditions"].split("|"):
                matches = gilda.ground(condition)
                if matches:
                    valid = True
                    yield Node(
                        db_ns=matches[0].term.db, db_id=matches[0].term.id,
                        labels=[]
                    )

            if not pd.isna(row["Interventions"]):
                for intervention in row["Interventions"].split("|"):
                    if drug_pattern.match(intervention):
                        int_matches = gilda.ground(intervention[6:])
                        if int_matches:
                            valid = True
                            yield Node(
                                db_ns=int_matches[0].term.db,
                                db_id=int_matches[0].term.id,
                                labels=[]
                            )

            if valid:
                yield Node(
                    db_ns="CLINICALTRIALS",
                    db_id=row["URL"][32:],
                    labels=[]
                )

    def get_relations(self):
        for index, row in self.df.iterrows():
            for condition in row["Conditions"].split("|"):
                cond_matches = gilda.ground(condition)
                if cond_matches:
                    source_ns = cond_matches[0].term.db
                    source_id = cond_matches[0].term.id
                    if not pd.isna(row["Interventions"]):
                        for intervention in row["Interventions"].split("|"):
                            if drug_pattern.match(intervention):
                                int_matches = gilda.ground(intervention[6:])
                                if int_matches:
                                    target_ns = int_matches[0].term.db
                                    target_id = row["URL"][32:]
                                    yield Relation(
                                        source_ns=source_ns,
                                        source_id=source_id,
                                        target_ns=target_ns,
                                        target_id=target_id,
                                        rel_type="has_trial",
                                    )
                                    yield Relation(
                                        source_ns=source_ns,
                                        source_id=source_id,
                                        target_ns=target_ns,
                                        target_id=target_id,
                                        rel_type="tested_in",
                                    )
