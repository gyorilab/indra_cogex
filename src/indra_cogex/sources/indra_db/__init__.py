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
        for side in 'AB':
            self.df[side] = [
                f'{prefix}:{identifier}'
                for prefix, identifier in self.df[[f'ag{side}_ns', f'ag{side}_id']].values
            ]

    def get_nodes(self):
        df = (
            pd.concat([self._get_nodes('A'), self._get_nodes('B')])
                .sort_values('curie')
                .drop_duplicates()
        )
        for curie, name in df.values:
            yield Node(curie, ['BioEntity'], dict(name=name))

    def _get_nodes(self, side):
        columns = {
            side: 'curie',
            f'ag{side}_name': 'name',
        }
        return (
            self.df[[side, f'ag{side}_name']]
                .drop_duplicates()
                .rename(columns=columns)
        )

    def get_relations(self):
        columns = ['A', 'B', 'stmt_type', 'evidence_count', 'stmt_hash']
        for source, target, stmt_type, ev_count, stmt_hash in self.df[columns].drop_duplicates().values:
            data = {'stmt_hash:int': stmt_hash, 'evidence_count:int': ev_count}
            yield Relation(source, target, [stmt_type], data)

    @staticmethod
    def _get_curie(row, agent_id):
        return f"{row[f'ag{agent_id}_ns']}:{row[f'ag{agent_id}_id']}"
