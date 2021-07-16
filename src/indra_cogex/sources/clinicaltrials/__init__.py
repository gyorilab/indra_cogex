import re
import pandas as pd
import gilda
from indra_cogex.sources.processor import Processor
from indra_cogex.representation import Node, Relation

drug_pattern = re.compile(r"^Drug: ([a-zA-Z ]|\d)+$")


class ClinicaltrialsProcessor(Processor):

    name = "clinicaltrials"

    def __init__(self, path):
        self.df = pd.read_csv(path, sep="\t")

    def get_nodes(self):
        for conditions in self.df["Conditions"]:
            for condition in conditions.split("|"):
                matches = gilda.ground(condition)
                if matches:
                    yield Node(
                        db_ns=matches[0].term.db, db_id=matches[0].term.id, labels=[]
                    )

        for interventions in self.df["Interventions"]:
            if not pd.isna(interventions):
                for intervention in interventions.split("|"):
                    if drug_pattern.match(intervention):
                        matches = gilda.ground(intervention[6:])
                        if matches:
                            yield Node(
                                db_ns=matches[0].term.db,
                                db_id=matches[0].term.id,
                                labels=[],
                            )

    def get_relations(self):
        for conditions in self.df["Conditions"]:
            for condition in conditions.split("|"):
                cond_matches = gilda.ground(condition)
                if cond_matches:
                    for interventions in self.df["Interventions"]:
                        if not pd.isna(interventions):
                            for intervention in interventions.split("|"):
                                if drug_pattern.match(intervention):
                                    int_matches = gilda.ground(intervention[6:])
                                    if int_matches:
                                        yield Relation(
                                            source_ns=cond_matches[0].term.db,
                                            source_id=cond_matches[0].term.id,
                                            target_ns=int_matches[0].term.db,
                                            target_id=int_matches[0].term.id,
                                            rel_type="has_trial",
                                        )
                                        yield Relation(
                                            source_ns=cond_matches[0].term.db,
                                            source_id=cond_matches[0].term.id,
                                            target_ns=int_matches[0].term.db,
                                            target_id=int_matches[0].term.id,
                                            rel_type="tested_in",
                                        )
