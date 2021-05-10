# -*- coding: utf-8 -*-

from class_resolver import Resolver

from .bgee import BgeeProcessor
from .go import GoProcessor
from .indra_db import DbProcessor
from .indra_ontology import OntologyProcessor
from .pathways import ReactomeProcessor, WikipathwaysProcessor
from .processor import Processor

__all__ = [
    "processor_resolver",
    "Processor",
    "BgeeProcessor",
    "GoProcessor",
    "DbProcessor",
    "OntologyProcessor",
]

processor_resolver = Resolver.from_subclasses(Processor)
