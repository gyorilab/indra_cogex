# -*- coding: utf-8 -*-

from .bgee import BgeeProcessor
from .indra_db import DbProcessor
from .indra_ontology import OntologyProcessor
from .processor import Processor

__all__ = [
    'Processor',
    'BgeeProcessor',
    'DbProcessor',
    'OntologyProcessor',
]
