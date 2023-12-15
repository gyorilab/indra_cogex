import pandas as pd
import pystow
from indra_cogex.representation import Node, Relation
from indra_cogex.sources import Processor
import gilda

GWAS_URL = "https://www.ebi.ac.uk/gwas/api/search/downloads/full"
SUBMODULE = pystow.module("indra", "cogex", "gwas")


__all__ = [
    "GWASProcessor",
]


class GWASProcessor(Processor):
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
        columns = ["SNPS", "phenotype_prefix", "phenotype_id", "phenotype_name"]

        for snp_id, phenotype_prefix, phenotype_id, phenotype_name in (
            self.df[columns].drop_duplicates().values
        ):
            yield Relation(
                "DBSNP",
                snp_id,
                phenotype_prefix,
                phenotype_id,
                self.relation
            )


def load_data(
    url: str,
    force: bool = False,
) -> pd.DataFrame:
    df_ = SUBMODULE.ensure_csv(url=url, name="associations.tsv", force=force)
    df_ = ground_phenotype_descriptions(df_)
    return df_


def ground_phenotype_descriptions(df_: pd.DataFrame):
    scored_match_iterator = df_["DISEASE/TRAIT"].map(
        lambda disease_trait: gilda.ground(disease_trait)
    )

    phenotype_prefix_list = []
    phenotype_id_list = []
    phenotype_name_list = []

    for scored_match_list in scored_match_iterator:
        phenotype_prefix_list.append(
            extract_prefix_from_matched_term(scored_match_list)
        )
        phenotype_id_list.append(extract_id_from_matched_term(scored_match_list))
        phenotype_name_list.append(extract_normalized_name(scored_match_list))

    df_["phenotype_prefix"] = phenotype_prefix_list
    df_["phenotype_id"] = phenotype_id_list
    df_["phenotype_name"] = phenotype_name_list

    df_ = df_[df_["phenotype_prefix"].notna()]
    return df_


def extract_prefix_from_matched_term(scored_match_list):
    if scored_match_list:
        return scored_match_list[0].term.db
    else:
        return None


def extract_id_from_matched_term(scored_match_list):
    if scored_match_list:
        return scored_match_list[0].term.id
    else:
        return None


def extract_normalized_name(scored_match_list):
    if scored_match_list:
        return scored_match_list[0].term.norm_text
    else:
        return None


