# -*- coding: utf-8 -*-

from class_resolver import Resolver

from .bgee import BgeeProcessor
from .indra_db import DbProcessor
from .indra_ontology import OntologyProcessor
from .pathways import PyoboProcessor, ReactomeProcessor, WikipathwaysProcessor
from .processor import Processor

__all__ = [
    "processor_resolver",
    "Processor",
    "BgeeProcessor",
    "DbProcessor",
    "OntologyProcessor",
]

processor_resolver = Resolver.from_subclasses(
    Processor,
    skip={PyoboProcessor},
)
