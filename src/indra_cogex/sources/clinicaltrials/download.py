"""
Download and parse the ClinicalTrials.gov data using Trialsynth.
"""
# Todo:
#  1. Consider subclassing the Grounder and Annotator classes from trialsynth
#  2. Add metadata to the reference relationships about the type of reference:
#     "results", "background" or "derived".
import os
from typing import Union, Dict

import pystow
import pandas as pd

from indra.ontology.bio import bio_ontology
from trialsynth.clinical_trials_dot_gov import config, process

__all__ = [
    "ensure_clinical_trials_df",
    "process_trialsynth_edges",
    "process_trialsynth_bioentity_nodes",
    "process_trialsynth_trial_nodes",
]

CLINICAL_TRIALS_MODULE = pystow.module(
    "indra",
    "cogex",
    "clinicaltrials",
)
TRIALSYNTH_PATH = CLINICAL_TRIALS_MODULE.module("trialsynth")
os.environ["DATA_DIR"] = str(TRIALSYNTH_PATH.base.absolute())

# Initializes the configuration for the clinical trials module in trialsynth
ctconfig = config.CTConfig()

logger = logging.getLogger(__name__)


def ensure_clinical_trials_df(
    *,
    redownload: bool = False,
    reprocess: bool = False,
    max_pages: Optional[int] = None
):
    """Download and parse the ClinicalTrials.gov data using Trialsynth.

    Parameters
    ----------
    redownload :
        If True, redownload the raw data, even if it already exists. This will
        also set `reprocess` to True, since redownloading the data means that it
        needs to be reprocessed. Default: False.
    reprocess :
        If True, reprocess the data, even if it already exists. This will
        reprocess the raw data
    max_pages :
        The maximum number of pages to download from the ClinicalTrials.gov API.
        If None, all pages will be downloaded. Default: None.
    """
    if redownload:
        reprocess = True

    # Check the processed trialsynth data directory
    if not reprocess and all(
        p.exists() for p in (
            ctconfig.edges_path,
            ctconfig.bio_entities_path,
            ctconfig.trials_path,
        )
    ):
        logger.info("ClinicalTrials.gov data already processed, skipping download.")
        return

    ctp = process.CTProcessor(
        reload_api_data=redownload, store_samples=True, validate=False
    )
    ctp.run(max_pages=max_pages)


def _mesh_to_chebi(row) -> Union[str, None]:
    """Convert a MeSH CURIE to a ChEBI CURIE if possible for intervention edges"""
    mesh_curie = row.get("bioentity", None)
    if mesh_curie is None:
        return None
    if (not mesh_curie.lower().startswith("mesh:") or
        row["rel_type:string"] != "has_intervention"):
        # If it's not mesh or it's not an intervention row just return the
        # original CURIE
        return mesh_curie

    chebi_ns, chebi_id = bio_ontology.map_to(
        ns1="MESH", id1=mesh_curie.split(":")[1], ns2="CHEBI"
    ) or (None, None)
    # The bio_ontology has chebi nodes stored as ("CHEBI", "CHEBI:12345")
    return chebi_id if chebi_id else mesh_curie


def process_trialsynth_edges() -> pd.DataFrame:
    """Convert the edge file from the trialsynth to CoGEx format

    Returns
    -------
    :
        The converted edges DataFrame with CoGEx format headers and intervention
         values converted to Chebi identifiers.
    """
    headers_translation = {
        "from:CURIE": "trial",
        "to:CURIE": "bioentity",
    }

    # Read the edges file from trialsynth
    edges_df = pd.read_csv(ctconfig.edges_path, sep="\t", compression="gzip")

    # Rename the columns to match CoGEx format
    edges_df.rename(columns=headers_translation, inplace=True)

    # Only translate has_intervention edges from mesh to chebi, but use a new column
    # for the resulting values and fill in with the untranslated values for the rows
    # that were not translated
    edges_df["bioentity_mapped"] = edges_df.apply(_mesh_to_chebi, axis=1)

    # Drop the "source_registry:string" column
    if "source_registry:string" in edges_df.columns:
        edges_df.drop(columns=["source_registry:string"], inplace=True)

    return edges_df


def process_trialsynth_bioentity_nodes(mesh_chebi_map: Dict[str, str]) -> pd.DataFrame:
    """Convert the bioentity nodes file from the trialsynth to CoGEx format

    Returns
    -------
    :
        writeme
    """
    headers_translation = {
        "curie:CURIE": "bioentity",
        "term:string": "name",
    }

    # Read the bioentity nodes file from trialsynth
    bioentity_nodes_df = pd.read_csv(
        ctconfig.bio_entities_path, sep="\t", compression="gzip"
    )

    # Rename the columns to match CoGEx format
    bioentity_nodes_df.rename(columns=headers_translation, inplace=True)

    def _map_to_chebi(row) -> str:
        if "intervention" in row["labels:LABEL[]"].lower():
            return mesh_chebi_map.get(
                row["bioentity"], row["bioentity"]
            )
        return row["bioentity"]

    # Translate the same rows that were translated in the edges file:
    # Any mesh id that is an intervention should be attempted to be converted to chebi
    bioentity_nodes_df["bioentity_mapped"] = bioentity_nodes_df.apply(_map_to_chebi, axis=1)

    # Map names for the chebi mapped bioentities
    def _nsid_to_name(row) -> str:
        if pd.isna(row["bioentity_mapped"]) or row["bioentity_mapped"] is None:
            return row["name"]
        if "intervention" in row["labels:LABEL[]"].lower() and \
            row["bioentity_mapped"].lower().startswith("chebi:"):
            # If it's an intervention, get the name from the chebi id, use the
            # existing name as default
            mapped_name = bio_ontology.get_name(
                ns="CHEBI", id=row["bioentity_mapped"].upper()
            )
            return mapped_name or row["name"]
        return row["name"]

    bioentity_nodes_df["name"] = bioentity_nodes_df.apply(_nsid_to_name, axis=1)

    return bioentity_nodes_df[["bioentity_mapped", "name"]]


def process_trialsynth_trial_nodes() -> pd.DataFrame:
    """Convert the trial nodes file from the trialsynth to CoGEx format

    Returns
    -------
    :
        writeme
    """
    # Cogex headers:
    # id:ID
    # :LABEL
    # phase:int  <-- -1 for unknown phase
    # randomized:boolean
    # start_year:int
    # start_year_anticipated:boolean
    # status  # Completed, terminated etc...
    # study_type  # Observational, interventional etc...
    # why_stopped

    # Trialsynth trial nodes file has the following columns:
    # curie:CURIE
    # title:string
    # labels:LABEL[] <-- Contains 'Allocation: RANDOMIZED' if randomized
    # design:DESIGN
    # conditions:CURIE[]
    # interventions:CURIE[]
    # primary_outcome:OUTCOME[]
    # secondary_outcome:OUTCOME[]
    # secondary_ids:CURIE[]
    # source_registry:string
    # phases:PHASE[]
    # start_year:int
    # labels:LABEL[]
    # why_stopped:string

    # Translate the headers to CoGEx format, these map 1:1
    headers_translation = {
        "curie:CURIE": "id:ID",
        "why_stopped:string": "why_stopped",
        "status:string": "status",
    }

    def _get_phase(phase_string: str) -> int:
        if pd.notna(phase_string):
            # The phase string is a list of phases, e.g. "PHASE1|PHASE2"
            # Get the highest phase number
            max_phase = max(
                int(p[-1]) if p[-1].isdigit() else -1
                for p in phase_string.split("|")
            )
            return max_phase
        return -1

    # Read the trial nodes file from trialsynth
    trials_nodes_df = pd.read_csv(ctconfig.trials_path, sep="\t", compression="gzip")

    # Rename the columns to match CoGEx format
    trials_nodes_df.rename(columns=headers_translation, inplace=True)

    # Add the :LABEL column
    trials_nodes_df[":LABEL"] = "ClinicalTrial"

    # Add the phase column, defaulting to -1 (unknown), pick the max phase
    trials_nodes_df["phase:int"] = trials_nodes_df["phases:PHASE[]"].apply(_get_phase)

    # Set the randomized boolean column based on the labels containing 'Allocation: RANDOMIZED'
    trials_nodes_df["randomized:boolean"] = trials_nodes_df["labels:LABEL[]"].str.contains(
        "Allocation: RANDOMIZED")

    # Add the start_year:int column, defaulting to 0 (unknown)
    trials_nodes_df["start_year"] = trials_nodes_df["start_year:integer"].apply(
        lambda x: int(x) if pd.notna(x) else None
    ).astype("Int64")

    # fixme: need a better way to get the study type out
    def _get_study_type(labels: str) -> str:
        if pd.isna(labels):
            return "unknown"
        # The study type is in the labels, e.g. "Study Type: Interventional"
        for label in labels.split(";"):
            for study_type in ["interventional", "observational", "expanded access"]:
                if study_type in label.lower():
                    return study_type
        return "unknown"
    # Add study type column
    trials_nodes_df["study_type"] = trials_nodes_df["labels:LABEL[]"].apply(_get_study_type)

    return trials_nodes_df


if __name__ == "__main__":
    ensure_clinical_trials_df(refresh=True)
