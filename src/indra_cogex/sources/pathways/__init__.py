import logging
from typing import ClassVar

import pyobo
import pyobo.api.utils
from indra_cogex.representation import Node, Relation
from indra_cogex.sources.processor import Processor
from pyobo.struct import has_part

logger = logging.getLogger(__name__)

__all__ = [
    'PyoboProcessor',
    'WikipathwaysProcessor',
    'ReactomeProcessor',
]


class PyoboProcessor(Processor):
    prefix: ClassVar[str]
    relation: ClassVar[Relation]
    relation_label: ClassVar[str]

    def get_nodes(self):
        # TODO add license
        version = pyobo.api.utils.get_version(self.prefix)
        for identifier, name in pyobo.get_id_name_mapping('wikipathways').items():
            yield Node(f'{self.prefix}:{identifier}', ['BioEntity'], dict(name=name, version=version))

    def get_relations(self):
        df = pyobo.get_filtered_relations_df(self.prefix, self.relation)
        for identifier, t_prefix, t_identifier in df.values:
            yield Relation(f'{self.prefix}:{identifier}', f'{t_prefix}:{t_identifier}', [self.relation_label])


class WikipathwaysProcessor(PyoboProcessor):
    name = 'wikipathways'
    prefix = 'wikipathways'
    relation = has_part
    relation_label = 'haspart'


class ReactomeProcessor(PyoboProcessor):
    name = 'reactome'
    prefix = 'reactome'
    relation = has_part
    relation_label = 'haspart'


if __name__ == '__main__':
    WikipathwaysProcessor.cli()
