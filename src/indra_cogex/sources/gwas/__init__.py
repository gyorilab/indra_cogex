# -*- coding: utf-8 -*-

"""Process GWAS, a curated collection of human genome-wide association studies to extract
variant-phenotype associations."""
import logging
import re

import gilda
import pystow
import pandas as pd

from indra_cogex.sources.utils import get_bool
from indra_cogex.representation import Node, Relation
from indra_cogex.sources.processor import Processor

__all__ = [
    "GWASProcessor",
]

logger = logging.getLogger(__name__)

SUBMODULE = pystow.module("indra", "cogex", "gwas")
GWAS_URL = "https://www.ebi.ac.uk/gwas/api/search/downloads/full"


class GWASProcessor(Processor):
    """Processor for the GWAS database."""

    name = "gwas"
    node_types = ["BioEntity"]
    relation = "variant_phenotype_association"

    def __init__(self):
        self.df = load_data(GWAS_URL)

    def get_nodes(self):
        phenotypes_set = {
            (prefix, identifier, name)
            for prefix, identifier, name in self.df[
                ["phenotype_prefix", "phenotype_id", "phenotype_name"]
            ].values
        }

        for prefix, identifier, name in phenotypes_set:
            yield Node.standardized(
                db_ns=prefix, db_id=identifier, name=name, labels=["BioEntity"]
            )

        for dbsnp_id in self.df["SNPS"].unique():
            yield Node.standardized(
                db_ns="DBSNP", db_id=dbsnp_id, name=dbsnp_id, labels=["BioEntity"]
            )

    def get_relations(self):
        columns = [
            "SNPS",
            "phenotype_prefix",
            "phenotype_id",
            "phenotype_name",
            "PUBMEDID",
            "CONTEXT",
            "INTERGENIC",
            "P-VALUE",
        ]

        for (
            snp_id,
            phenotype_prefix,
            phenotype_id,
            phenotype_name,
            pmid,
            context,
            intergenic,
            p_value,
        ) in (
            self.df[columns].drop_duplicates().values
        ):
            data = {
                "gwas_pmid:int": pmid,
                "gwas_context": context,
                "gwas_intergenic:boolean": get_bool(intergenic),
                "gwas_p_value:float": p_value,
            }

            yield Relation(
                "DBSNP", snp_id, phenotype_prefix, phenotype_id, self.relation, data
            )


def load_data(
    url: str,
    force: bool = False,
) -> pd.DataFrame:
    df = SUBMODULE.ensure_csv(url=url, name="associations.tsv", force=force)
    df = map_phenotypes(df)
    return df


def map_phenotypes(df: pd.DataFrame) -> pd.DataFrame:
    (
        df["phenotype_prefix"],
        df["phenotype_id"],
        df["phenotype_name"],
    ) = zip(*df["DISEASE/TRAIT"].map(extract_phenotype_info))

    # filter out phenotypes that cannot be grounded
    df = df[df["phenotype_prefix"].notna()]
    
    # Filter out all snp ids that don't begin with "rs"
    # Around 10,000 entries contain snp ids in the "chr" (chromosome position) format
    # TODO: Convert snp ids beginning with "chr" to "rs" format
    df = df[df["SNPS"].map(lambda snp_id: bool(re.match("^rs\d+$", snp_id)))]

    return df


def extract_phenotype_info(phenotype: str):
    scored_match_list = gilda.ground(phenotype)
    if scored_match_list:
        return (
            scored_match_list[0].term.db,
            scored_match_list[0].term.id,
            scored_match_list[0].term.norm_text,
        )
    else:
        return None, None, None
