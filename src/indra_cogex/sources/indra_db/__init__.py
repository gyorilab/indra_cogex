import logging
import pickle

import humanize
import pandas as pd
import pystow

from indra_cogex.representation import Node, Relation
from indra_cogex.sources.processor import Processor

logger = logging.getLogger(__name__)


# If you don't have the data, get it from:
# 's3://bigmech/indra-db/dumps/2021-01-26/sif.pkl'

class DbProcessor(Processor):
    name = 'database'
    df: pd.DataFrame

    def __init__(self, path=None):
        if path is None:
            path = pystow.join('indra', 'db', name='sif.pkl')
        with open(path, 'rb') as fh:
            df = pickle.load(fh)
        logger.info('Loaded %s rows from %s', humanize.intword(len(df)), path)
        self.df = df

    def get_nodes(self):
        for _, row in self.df.iterrows():
            for agent_id in ['A', 'B']:
                identifier = self._get_curie(row, agent_id)
                yield Node(identifier, ['BioEntity'])

    def get_relations(self):
        for _, row in self.df.iterrows():
            source = self._get_curie(row, 'A')
            target = self._get_curie(row, 'B')
            edge_type = row['stmt_type']
            data = {k: row[k] for k in {'stmt_hash', 'evidence_count'}}
            yield Relation(source, target, [edge_type], data)

    @staticmethod
    def _get_curie(row, agent_id):
        return f"{row[f'ag{agent_id}_ns']}:{row[f'ag{agent_id}_id']}"
