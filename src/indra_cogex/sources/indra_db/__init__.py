import pickle
import logging
from indra_cogex.representation import Node, Relation


logger = logging.getLogger(__name__)


class Processor:
    def __init__(self, path):
        with open(path, 'rb') as fh:
            df = pickle.load(fh)
        logger.info('Loaded %d rows from %s' % (len(df), path))
        self.df = df

    def get_nodes(self):
        for _, row in self.df.iterrows():
            for agent_id in ['A', 'B']:
                identifier = f"{row[f'ag{agent_id}_ns']}:" \
                             f"{row[f'ag{agent_id}_id']}"
                yield Node(identifier, ['BioEntity'])

    def get_relations(self):
        for _, row in self.df.iterrows():
            source = f"{row['agA_ns']}:{row['agA_id']}"
            target = f"{row['agB_ns']}:{row['agB_id']}"
            edge_type = row['stmt_type']
            data = {k: row[k] for k in {'stmt_hash', 'evidence_count'}}
            yield Relation(source, target, [edge_type], data)
