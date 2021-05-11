# -*- coding: utf-8 -*-

"""Processors for generating nodes and relations for upload to Neo4j."""

from class_resolver import Resolver

from .bgee import BgeeProcessor
from .go import GoaProcessor
from .indra_db import DbProcessor
from .indra_ontology import OntologyProcessor
from .pathways import ReactomeProcessor, WikipathwaysProcessor
from .processor import Processor

__all__ = [
    "processor_resolver",
    "Processor",
    "BgeeProcessor",
    "ReactomeProcessor",
    "WikipathwaysProcessor",
    "GoaProcessor",
    "DbProcessor",
    "OntologyProcessor",
]

processor_resolver = Resolver.from_subclasses(Processor)
