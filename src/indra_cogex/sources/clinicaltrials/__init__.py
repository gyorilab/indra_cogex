import pandas as pd
import gilda
from indra_cogex.sources.processor import Processor
from indra_cogex.representation import Node, Relation


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

    def get_relations(self):
        # TODO: Replace this placeholder implementation
        yield Relation(
            source_ns="a", source_id="1", target_ns="b", target_id="2", rel_type="r"
        )
