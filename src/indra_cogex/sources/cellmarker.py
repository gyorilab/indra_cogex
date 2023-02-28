# -*- coding: utf-8 -*-

"""Processor for the CellMarker database.

.. seealso::

    - Website: http://xteam.xbio.top/CellMarker/
    - Publication: https://doi.org/10.1093/nar/gky900
"""

import logging
from typing import Optional

import pandas as pd
import pyobo
from indra.databases import hgnc_client
from indra.ontology.standardize import standardize_name_db_refs
from indra.statements.agent import get_grounding

from .processor import Processor
from ..representation import Node, Relation

__all__ = [
    "CellMarkerProcessor",
]

logger = logging.getLogger(__name__)

URL = "http://xteam.xbio.top/CellMarker/download/Human_cell_markers.txt"

#: These are manual groundings for cancer types,
#: in case we ever want to include them in the future
CANCER_GROUNDINGS = {
    "Brain Cancer": ("DOID", "DOID:1319"),
    "Vascular Tumour": ("DOID", "DOID:175"),
    "Natural Killer Cell Lymphoma": ("DOID", "DOID:0050743"),
    # Needed to map to less specific mesh term
    "Breast Cancer (circulating)": ("MESH", "D001943"),  # not so specific
    "Gastric Cancer (circulating)": ("MESH", "D013274"),
    "Melanoma (circulating)": ("MESH", "D008545"),
    "Non-small Cell Lung Cancer (circulating)": ("MESH", "D002289"),
    "Prostate Cancer (circulating)": ("MESH", "D011471"),
    "Testicular Germ Cell Tumor (circulating)": ("MESH", "C563236"),
    # Could not find something
    "Oesophageal Cancer": "",
    "B cell type MALT lymphoma": "",
}


class CellMarkerProcessor(Processor):
    """Processor for the CellMarker database."""

    name = "cellmarker"
    df: pd.DataFrame
    node_types = ["BioEntity"]
    rel_type = "has_marker"

    def __init__(self, df: Optional[pd.DataFrame] = None):
        """Initialize the CellMarker processor."""
        self.df = get_df() if df is None else df

    def get_nodes(self):  # noqa:D102
        """Get cell, tissue, and gene nodes."""
        for cl_id in sorted(self.df["cl"].unique()):
            yield Node.standardized(
                db_ns="CL",
                db_id=f"CL:{cl_id}",
                name=pyobo.get_name("cl", cl_id),
                labels=["BioEntity"],
            )
        for hgnc_id in sorted(self.df["hgnc"].unique()):
            yield Node.standardized(
                db_ns="HGNC",
                db_id=hgnc_id,
                name=hgnc_client.get_hgnc_name(hgnc_id),
                labels=["BioEntity"],
            )

    def get_relations(self):  # noqa:D102
        columns = ["cl", "hgnc", "uberon", "pubmed", "marker_resource"]
        for (cl, hgnc), sdf in self.df[columns].groupby(["cl", "hgnc"]):
            all_tissues = set()
            all_pubmeds = set()
            all_markers = set()
            for _, _, uberon, pubmed, marker in sdf.values:
                all_tissues.add(uberon)
                all_pubmeds.add(pubmed)
                all_markers.add(marker)

            data = {
                "pubmed:string[]": _join(all_pubmeds),
                "markers:string[]": _join(all_markers),
                "tissue_uberon_ids": _join(all_tissues),
            }
            yield Relation(
                "CL",
                f"CL:{cl}",
                "HGNC",
                hgnc,
                self.rel_type,
                data,
            )


def get_df(url: str = URL) -> pd.DataFrame:
    """Get the CellMarker dataframe."""
    df = pd.read_csv(url, sep="\t", dtype=str)

    # Remove redundant species type annotation since we're looking at human markers file
    del df["speciesType"]

    # Assert existence of and clean CL identifier
    df = df[df["CellOntologyID"].notna()]
    df["cl"] = df["CellOntologyID"].map(_get_obo_luid)
    del df["CellOntologyID"]
    del df["cellName"]

    # Assert existence of and clean UBERON identifier
    df = df[df["UberonOntologyID"].notna()]
    df["uberon"] = df["UberonOntologyID"].map(_get_obo_luid)
    del df["UberonOntologyID"]
    del df["tissueType"]

    # this is either "Normal cell" or "Cancer cell" and is redundant
    # with df["cancerType"]
    del df["cellType"]

    # Remove non-normal cell types
    df = df[df["cancerType"] == "Normal"]
    del df["cancerType"]

    # not enough information here
    del df["Company"]

    # Redundant of genes
    del df["proteinName"]
    del df["proteinID"]

    # df["cellMarker"] appears to be the un-normalized version o df["geneSymbol"]
    del df["cellMarker"]
    del df["geneSymbol"]

    # split comma-separated lists of entrez gene ids
    df["hgnc_ids"] = df["geneID"].map(_parse_ncbigenes)
    del df["geneID"]

    # take all of the cells that contain lists of HGNC
    # identifirs and make them their own rows
    df = df.explode(["hgnc_ids"])
    df = df.rename(
        columns={
            "PMID": "pubmed",
            "markerResource": "marker_resource",
            "hgnc_ids": "hgnc",
        }
    )
    # Remove any rows where the HGNC ID wasn't mapped to something
    df = df[df["hgnc"].notna()]
    return df


def _get_obo_luid(s: str) -> str:
    return s.split("_", 1)[1]


def _parse_ncbigenes(s):
    """Parse string containing comma-separated NCBIGene identifiers."""
    if pd.isna(s):
        return []
    ncbigene_ids = (ncbigene_id.strip() for ncbigene_id in s.strip().split(","))
    hgnc_ids = (
        hgnc_client.get_hgnc_from_entrez(ncbigene_id)
        for ncbigene_id in ncbigene_ids
    )
    return sorted(hgnc_id for hgnc_id in hgnc_ids if hgnc_id)


def ground_cancer_types(df: pd.DataFrame):
    """
    A function that takes the CellMarker dataframe
    and attempts to ground cancer types.
    """
    import gilda

    rows = []
    for x in sorted(df["cancerType"].unique()):
        if x == "Normal":
            rows.append((x, None, None, None))
            continue

        prefix, identifier = CANCER_GROUNDINGS.get(x) or (None, None)
        if prefix:
            name, db_xrefs = standardize_name_db_refs({prefix: identifier})
        else:
            scored_matches = gilda.ground(x)
            if not scored_matches:
                rows.append((x, None, None, None))
                continue
            prefix = scored_matches[0].term.db
            identifier = scored_matches[0].term.id

            name, db_xrefs = standardize_name_db_refs({prefix: identifier})
            if not db_xrefs:
                # print(f"No standardization for: {x} ({scored_matches[0].term.db}:{scored_matches[0].term.id})")
                rows.append((x, None, None, None))
                continue
        prefix, identifier = get_grounding(db_xrefs)
        rows.append((x, prefix, identifier, name))


def _join(collection):
    return "|".join(item for item in collection if item and pd.notna(item))


if __name__ == "__main__":
    CellMarkerProcessor.cli()
