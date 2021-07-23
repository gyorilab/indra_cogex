import re
import pandas as pd
import gilda
from xml.etree import ElementTree as ET
from indra_cogex.sources.processor import Processor
from indra_cogex.representation import Node, Relation

# drug_pattern = re.compile(r"^Drug: (.+)$")


class ClinicaltrialsProcessor(Processor):
    name = "clinicaltrials"

    def __init__(self, path):
        # self.df = pd.read_csv(path, sep="\t")
        self.tree = ET.parse(path)
        self.has_trial_cond_ns = []
        self.has_trial_cond_id = []
        self.has_trial_nct = []
        self.tested_in_int_ns = []
        self.tested_in_int_id = []
        self.tested_in_nct = []

    def get_nodes(self):
        for study in self.tree.findall("study"):
            valid = False
            for condition in study[3]:
                cond_matches = gilda.ground(str(condition))
                if cond_matches:
                    valid = True
                    self.has_trial_cond_ns.append(cond_matches[0].term.db)
                    self.has_trial_cond_id.append(cond_matches[0].term.id)
                    self.has_trial_nct.append(study[6].text[32:])
                    yield Node(
                        db_ns=cond_matches[0].term.db,
                        db_id=cond_matches[0].term.id,
                        labels=[],
                    )

            for intervention in study[4]:
                if intervention.attrib["type"] == "Drug":
                    int_matches = gilda.ground(str(intervention))
                    if int_matches:
                        valid = True
                        self.tested_in_int_ns.append(int_matches[0].term.db)
                        self.tested_in_int_id.append(int_matches[0].term.id)
                        self.tested_in_nct.append(study[6].text[32:])
                        print(int_matches[0])
                        yield Node(
                            db_ns=int_matches[0].term.db,
                            db_id=int_matches[0].term.id,
                            labels=[],
                        )

            if valid:
                yield Node(db_ns="CLINICALTRIALS", db_id=study[6].text[32:], labels=[])

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
