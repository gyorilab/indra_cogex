# -*- coding: utf-8 -*-

"""Processors for generating nodes and relations for upload to Neo4j."""

from class_resolver import Resolver

from .bgee import BgeeProcessor
from .cbioportal import (
    CcleCnaProcessor,
    CcleDrugResponseProcessor,
    CcleMutationsProcessor,
)
from .chembl import ChemblIndicationsProcessor
from .clinicaltrials import ClinicaltrialsProcessor
from .goa import GoaProcessor
from .indra_db import DbProcessor
from .indra_ontology import OntologyProcessor
from .pathways import ReactomeProcessor, WikipathwaysProcessor
from .processor import Processor
from .sider import SIDERSideEffectProcessor

__all__ = [
    "processor_resolver",
    "Processor",
    "BgeeProcessor",
    "ReactomeProcessor",
    "WikipathwaysProcessor",
    "GoaProcessor",
    "DbProcessor",
    "OntologyProcessor",
    "CcleCnaProcessor",
    "CcleMutationsProcessor",
    "CcleDrugResponseProcessor",
    "ClinicaltrialsProcessor",
    "ChemblIndicationsProcessor",
    "SIDERSideEffectProcessor",
]

processor_resolver = Resolver.from_subclasses(Processor)
