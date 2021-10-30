# -*- coding: utf-8 -*-

"""Processor for SIDER."""

import re
from collections import Counter
from typing import Iterable

import gilda
import gilda.grounder
import pandas as pd
import pyobo
import pystow
from biomappings import load_mappings
from tabulate import tabulate
from tqdm import tqdm

from indra.databases import biolookup_client
from indra.databases.identifiers import get_ns_id_from_identifiers
from indra_cogex.representation import Node, Relation, standardize
from indra_cogex.sources import Processor


VERSION = "4.1"
SUBMODULE = pystow.module("indra", "cogex", "sider", VERSION)
URL = "http://sideeffects.embl.de/media/download/meddra_all_se.tsv.gz"
SIDE_EFFECTS_HEADER = [
    # 'STITCH_FLAT_ID',
    "STITCH_STEREO_ID",
    # 'UMLS CUI from Label',
    "MedDRA Concept Type",
    "UMLS CUI from MedDRA",
    "MedDRA Concept name",
]


cid_to_pubchem_pattern = re.compile(r"^CID(?:0)+(\d+)$")


def stitch_stereo_to_pubchem(cid: str) -> str:
    assert cid.startswith("CID")
    return re.sub(cid_to_pubchem_pattern, "\\1", cid)


class UmlsMapper:
    """A utility class for mapping out of UMLS."""

    prefixes = ["doid", "mesh", "hp", "efo", "mondo"]

    def __init__(self):
        """Prepare the UMLS mappings from PyOBO and Biomappings."""
        #: A dictionary from external prefix to UMLS id to external ID
        self.xrefs = {}

        for prefix in self.prefixes:
            self.xrefs[prefix] = {}
            # Get external to UMLS
            for external_id, umls_id in pyobo.get_filtered_xrefs(
                prefix, "umls"
            ).items():
                self.xrefs[prefix][umls_id] = external_id
            # Get UMLS to external
            for umls_id, external_id in pyobo.get_filtered_xrefs(
                "umls", prefix
            ).items():
                self.xrefs[prefix][umls_id] = external_id

        # Get manually curated UMLS mappings from biomappings
        biomappings_from_umls, biomappings_to_umls = Counter(), Counter()
        for mapping in load_mappings():
            if mapping["source prefix"] == "umls":
                target_prefix = mapping["target prefix"]
                biomappings_from_umls[target_prefix] += 1
                target_id = mapping["target identifier"]
                source_id = mapping["source identifier"]
                if target_prefix in self.xrefs:
                    self.xrefs[target_prefix][target_id] = source_id
                else:
                    self.xrefs[target_prefix] = {
                        target_id: source_id,
                    }
            elif mapping["target prefix"] == "umls":
                source_prefix = mapping["source prefix"]
                biomappings_to_umls[source_prefix] += 1
                source_id = mapping["source identifier"]
                target_id = mapping["target identifier"]
                if source_prefix in self.xrefs:
                    self.xrefs[source_prefix][source_id] = target_id
                else:
                    self.xrefs[source_prefix] = {
                        source_id: target_id,
                    }

        print("Mapping out of UMLS")
        print(tabulate(biomappings_from_umls.most_common()))
        print("Mapping into UMLS")
        print(tabulate(biomappings_to_umls.most_common()))

        print("Total xrefs")
        print(
            tabulate(
                [(prefix, len(self.xrefs[prefix])) for prefix in self.prefixes],
                headers=["Prefix", "Mappings"],
            )
        )

    def lookup(self, umls_id: str):
        for prefix in self.prefixes:
            xrefs = self.xrefs[prefix]
            identifier = xrefs.get(umls_id)
            if identifier is not None:
                return standardize(prefix, identifier)
        return "umls", umls_id, pyobo.get_name("umls", umls_id)


class SIDERSideEffectProcessor(Processor):
    """A processor for SIDER side effects."""

    name = "sider_side_effects"

    def __init__(self):
        self.df = SUBMODULE.ensure_csv(
            url=URL,
            read_csv_kwargs=dict(
                usecols=[1, 3, 4, 5],
                names=SIDE_EFFECTS_HEADER,
                dtype=str,
            ),
        )
        self.df = self.df[self.df["STITCH_STEREO_ID"].notna()]
        self.df = self.df[self.df["UMLS CUI from MedDRA"].notna()]

        # Prepare chemicals
        self.df["pubchem_id"] = self.df["STITCH_STEREO_ID"].map(
            stitch_stereo_to_pubchem
        )
        del self.df["STITCH_STEREO_ID"]
        self.chemicals = {
            pubchem_id: Node.standardized(
                db_ns="PUBCHEM",
                db_id=pubchem_id,
                labels=["BioEntity"],
            )
            for pubchem_id in tqdm(
                self.df["pubchem_id"], unit_scale=True, desc="Caching chemicals"
            )
        }
        for node in tqdm(self.chemicals.values(), desc="Finding chemical names"):
            if node.data["name"] is None:
                node.data["name"] = biolookup_client.get_name(node.db_ns, node.db_id)

        umls_mapper = UmlsMapper()
        self.side_effects = {}
        for umls_id in self.df["UMLS CUI from MedDRA"].unique():
            prefix, identifier, name = umls_mapper.lookup(umls_id)
            db_ns, db_id = get_ns_id_from_identifiers(prefix, identifier)
            if db_ns is None:
                db_ns, db_id = prefix, identifier
            self.side_effects[umls_id] = Node.standardized(
                db_ns=db_ns, db_id=db_id, name=name, labels=["BioEntity"]
            )

    def get_nodes(self) -> Iterable[Node]:
        """Iterate over SIDER chemicals and side effects."""
        yield from self.chemicals.values()
        yield from self.side_effects.values()

    def get_relations(self) -> Iterable[Relation]:
        """Iterate over SIDER side effect annotations."""
        for pubchem_id, umls_id in self.df[
            ["pubchem_id", "UMLS CUI from MedDRA"]
        ].values:
            chemical = self.chemicals[pubchem_id]
            indication = self.side_effects[umls_id]
            yield Relation(
                chemical.db_ns,
                chemical.db_id,
                indication.db_ns,
                indication.db_id,
                "has_side_effect",
                dict(
                    source=self.name,
                    version=VERSION,
                ),
            )


def generate_curation_sheet():
    """This function does some statistics over the UMLS mappings out of SIDER and generates
    a curation sheet based on gilda when possible. Still, about half of the terms are only
    available in UMLS at the moment.
    """
    umls_mapper = UmlsMapper()

    df = SUBMODULE.ensure_csv(
        url=URL,
        read_csv_kwargs=dict(
            usecols=[1, 3, 4, 5],
            names=SIDE_EFFECTS_HEADER,
            dtype=str,
        ),
    )
    df["pubchem_id"] = df["STITCH_STEREO_ID"].map(stitch_stereo_to_pubchem)
    del df["STITCH_STEREO_ID"]

    print(tabulate(df.head(), headers=df.columns))
    umls_ids = df["UMLS CUI from MedDRA"].unique()
    unique_umls_id_count = len(umls_ids)
    print("Unique UMLS identifiers:", unique_umls_id_count)
    c = Counter()
    unnormalized = set()
    for umls_id in tqdm(umls_ids):
        db_ns, db_id, name = umls_mapper.lookup(umls_id)
        if db_ns is not None:
            c[db_ns] += 1
        else:
            unnormalized.add(umls_id)

    print("Most common prefixes mapped from UMLS to")
    print(tabulate(c.most_common()))

    number_normalized = sum(c.values())
    print(
        f"normalized  : {number_normalized}/{unique_umls_id_count} ({number_normalized / unique_umls_id_count:.2%})"
    )
    print(
        f"unnormalized: {len(unnormalized)}/{unique_umls_id_count} ({len(unnormalized) / unique_umls_id_count:.2%})"
    )

    print("mapping all rows")
    df["prefix"], df["identifier"], df["name"] = zip(
        *df["UMLS CUI from MedDRA"].map(umls_mapper.lookup)
    )
    df.to_csv(SUBMODULE.join(name="norm_df.tsv"), sep="\t", index=False)

    df_unnormalized = df.loc[df["prefix"].isna()]
    unique_unnormalized = {
        (umls_id, name)
        for umls_id, name in df_unnormalized[
            ["UMLS CUI from MedDRA", "MedDRA Concept name"]
        ].values
        if pd.notna(name) and pd.notna(umls_id)
    }
    rows = []
    for umls_id, name in sorted(unique_unnormalized):
        for result in gilda.ground(name):
            result: gilda.grounder.ScoredMatch
            term: gilda.grounder.Term = result.term
            rows.append(
                (
                    umls_id,
                    name,
                    term.db,
                    term.id,
                    term.entry_name,
                    result.score,
                )
            )
    curation_df = pd.DataFrame(
        rows, columns=["umls_id", "umls_name", "prefix", "identifier", "name", "score"]
    )
    curation_df.to_csv(SUBMODULE.join(name="curation_df.tsv"), sep="\t", index=False)


if __name__ == "__main__":
    SIDERSideEffectProcessor.cli()
