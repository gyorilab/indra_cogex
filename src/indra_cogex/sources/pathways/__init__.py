# -*- coding: utf-8 -*-

"""Processors for pathway databases."""

import logging
from typing import ClassVar

from indra.databases import hgnc_client
from indra.databases import uniprot_client
from indra.databases.identifiers import get_ns_id_from_identifiers
import pyobo
import pyobo.api.utils
from pyobo.struct import has_part

from indra_cogex.representation import Node, Relation
from indra_cogex.sources.processor import Processor

logger = logging.getLogger(__name__)

__all__ = [
    "WikipathwaysProcessor",
    "ReactomeProcessor",
]


class PyoboProcessor(Processor):
    name = "pyobo"
    prefix: ClassVar[str]
    relation: ClassVar[Relation]
    relation_label: ClassVar[str]
    importable = False

    def get_nodes(self):  # noqa:D102
        # TODO add license
        version = pyobo.api.utils.get_version(self.prefix)
        for identifier, name in pyobo.get_id_name_mapping("wikipathways").items():
            db_ns, db_id = get_ns_id_from_identifiers(self.prefix, identifier)
            yield Node(
                db_ns,
                db_id,
                ["BioEntity"],
                dict(name=name, version=version),
            )

    def get_relations(self):  # noqa:D102
        df = pyobo.get_filtered_relations_df(self.prefix, self.relation)
        for identifier, t_prefix, t_identifier in df.values:
            pathway_ns, pathway_id = get_ns_id_from_identifiers(self.prefix, identifier)
            gene_ns, gene_id = self.get_gene(t_prefix, t_identifier)
            if not gene_ns:
                continue
            yield Relation(
                pathway_ns,
                pathway_id,
                gene_ns,
                gene_id,
                [self.relation_label],
                dict(source=self.name),
            )

    def get_gene(self, prefix, identifier):
        if prefix == "ncbigene":
            hgnc_id = hgnc_client.get_hgnc_from_entrez(identifier)
            if hgnc_id:
                return "HGNC", hgnc_id
            else:
                return "EGID", identifier
        elif prefix == "uniprot":
            hgnc_id = uniprot_client.get_hgnc_id(identifier)
            if hgnc_id:
                return "HGNC", hgnc_id
            else:
                return "UP", identifier
        return None, None


class WikipathwaysProcessor(PyoboProcessor):
    """Processor for WikiPathways gene-pathway links."""

    name = "wikipathways"
    prefix = "wikipathways"
    relation = has_part
    relation_label = "haspart"
    importable = True


class ReactomeProcessor(PyoboProcessor):
    """Processor for Reactome gene-pathway links."""

    name = "reactome"
    prefix = "reactome"
    relation = has_part
    relation_label = "haspart"
    importable = True


if __name__ == "__main__":
    WikipathwaysProcessor.cli()
