"""
Download and parse the ClinicalTrials.gov data using Trialsynth.
"""
# Todo:
#  1. Consider subclassing the Grounder and Annotator classes from trialsynth
#  2. Add metadata to the reference relationships about the type of reference:
#     "results", "background" or "derived".
import logging
import os
from typing import Dict, Optional

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


#: Labels to keep for interventions in the bioentity nodes.
INTERVENTION_LABELS = {
    "DRUG",
    "DIETARY_SUPPLEMENT",
}


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


def _mesh_to_chebi(row) -> str:
    """Convert a MeSH CURIE to a ChEBI CURIE if possible for interventions"""
    # Some interventions in the trialsynth data are directly from mesh
    # annotations and don't go through grounding, where chebi is prioritized
    # over mesh
    curie = row["bioentity"]
    if curie is None:
        raise ValueError(
            "The row does not have a 'bioentity' column, cannot convert to CHEBI."
        )
    if row["rel_type:string"] == "condition" or not curie.lower().startswith("mesh:"):
        # If it's not mesh or it's not an intervention row just return the
        # original CURIE
        return curie

    # At this point we know that the CURIE is a MeSH CURIE and we want to
    # convert it to a ChEBI CURIE if possible.
    # The bio_ontology has chebi nodes stored as ("CHEBI", "CHEBI:12345")
    chebi_ns, chebi_id = bio_ontology.map_to(
        ns1="MESH", id1=curie.split(":")[1], ns2="CHEBI"
    ) or (None, None)
    # The bio_ontology has chebi nodes stored as ("CHEBI", "CHEBI:12345")
    return chebi_id if chebi_id else curie


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

    # Filter to interventions that are labeled as drugs or dietary supplements
    def _intervention_filter(row) -> bool:
        """Check if the row is an intervention that should be kept."""
        labels = row["labels:LABEL[]"].lower()
        if "condition" in labels:
            # We keep conditions as they are not interventions
            return True
        # Check if the labels contain any of the allowed intervention labels
        if any(label.lower() in labels for label in INTERVENTION_LABELS):
            return True
        # Check if the curie is from chebi
        if row["bioentity"].lower().startswith("chebi:"):
            return True
        return False

    # Filter the interventions to the ones that are labeled as drugs or dietary
    # supplements or where the curie is from chebi
    bioentity_nodes_df = bioentity_nodes_df[
        bioentity_nodes_df.apply(_intervention_filter, axis=1)
    ]

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
    # completion_year:int
    # completion_year_anticipated:boolean
    # last_update_submit_year:int
    # status  # Completed, terminated etc...
    # study_type  # Observational, interventional etc...
    # why_stopped

    # Trialsynth has the following headers:
    # curie:CURIE
    # title:string <- Corresponds to BriefTitle
    # official_title:string <- Corresponds to OfficialTitle
    # brief_summary:string,
    # detailed_description:string,
    # labels:LABEL[] - scsv of clinical_trials, intervention, observational, expanded_access
    # design:DESIGN - e.g. 'Purpose: PREVENTION; Allocation: RANDOMIZED;Masking: DOUBLE; Assignment: '
    # conditions:CURIE[] - scsv of mesh conditions, e.g. 'mesh:D000001;mesh:D000002'
    # interventions:CURIE[]
    # primary_outcome:OUTCOME[]
    # secondary_outcome:OUTCOME[]
    # secondary_ids:CURIE[]
    # source_registry:string
    # phases:PHASE[]
    # start_year:integer
    # start_year_anticipated:boolean
    # primary_completion_year:integer
    # primary_completion_year_type:string
    # completion_year:integer
    # completion_year_type:string
    # last_update_submit_year:integer
    # status:string
    # why_stopped:string
    # references:string[]

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

    # Set the start_year:int column
    trials_nodes_df["start_year"] = trials_nodes_df["start_year:integer"].apply(
        lambda x: int(x) if pd.notna(x) else None
    ).astype("Int64")

    # Set the completion_year column
    trials_nodes_df["completion_year"] = trials_nodes_df["completion_year:integer"].apply(
        lambda x: int(x) if pd.notna(x) else None
    ).astype("Int64")

    # Set the last_update_submit_year column
    trials_nodes_df["last_update_submit_year"] = trials_nodes_df["last_update_submit_year:integer"].apply(
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
