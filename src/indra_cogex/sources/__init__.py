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
from .indra_db import DbProcessor, EvidenceProcessor
from .indra_ontology import OntologyProcessor
from .pathways import ReactomeProcessor, WikipathwaysProcessor
from .processor import Processor
from .pubmed import PubmedProcessor
from .sider import SIDERSideEffectProcessor
from .hpoa import HpDiseasePhenotypeProcessor, HpPhenotypeGeneProcessor
from .nih_reporter import NihReporterProcessor

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
    "EvidenceProcessor",
    "PubmedProcessor",
    "HpDiseasePhenotypeProcessor",
    "HpPhenotypeGeneProcessor",
    "NihReporterProcessor"
]

processor_resolver = Resolver.from_subclasses(Processor)
