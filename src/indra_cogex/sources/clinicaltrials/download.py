"""
Download and parse the ClinicalTrials.gov data using Trialsynth.
"""
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

#: The fields that are used by default. A full list can be found
#: here: https://classic.clinicaltrials.gov/api/info/study_fields_list
DEFAULT_FIELDS = [
    "NCTId",
    "BriefTitle",
    "Condition",
    "ConditionMeshTerm",
    "ConditionMeshId",
    "InterventionName",
    "InterventionType",
    "InterventionMeshTerm",
    "InterventionMeshId",
    "StudyType",
    "DesignAllocation",
    "OverallStatus",
    "Phase",
    "WhyStopped",
    "SecondaryIdType",
    "SecondaryId",
    "StartDate",  # Month [day], year: "November 1, 2023", "May 1984" or NaN
    "StartDateType",  # "Actual" or "Anticipated" (or NaN)
    "ReferencePMID",  # these are tagged as relevant by the author, but not necessarily about the trial
]


def ensure_clinical_trials_df(*, refresh: bool = False):
    """Download and parse the ClinicalTrials.gov dataframe or load
    it, if it's already available.

    If refresh is set to true, it will overwrite the existing file.
    """
    ctp = process.CTProcessor(
        reload_api_data=refresh, store_samples=True, validate=False
    )
    ctp.run()


def _mesh_to_chebi(row) -> Union[str, None]:
    """Convert a MeSH CURIE to a ChEBI CURIE if possible for intervention edges"""
    mesh_curie = row.get("bioentity", None)
    if mesh_curie is None:
        return None
    if (not mesh_curie.lower().startswith("mesh:") or
        row["rel_type:string"] != "has_intervention"):
        # If it's not mesh or it's not an intervention row just return the
        # original CURIE
        return mesh_curie.lower()

    chebi_ns, chebi_id = bio_ontology.map_to(
        ns1="MESH", id1=mesh_curie.split(":")[1], ns2="CHEBI"
    ) or (None, None)
    # The bio_ontology has chebi nodes stored as ("CHEBI", "CHEBI:12345"),
    # CoGEx needs just "chebi:12345"
    return chebi_id.lower() if chebi_id else mesh_curie


def process_trialsynth_edges() -> pd.DataFrame:
    """Convert the edge file from the trialsynth to CoGEx format

    Returns
    -------
    :
        The converted edges DataFrame with CoGEx format headers and intervention
         values converted to Chebi identifiers.
    """
    headers_translation = {
        # The Trialsynth edges go from the trial to the bioentity with a
        # has_intervention or has_condition relation. In CoGEx the edge goes
        # from the bioentity to the trial with a tested_in or has_trial edge
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
    # Any mesh id that is an intervention should be converted to chebi
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
    # study_type  # Observational, interventional etc... Fixme: should be among labels but doesn't seem to be there
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
        lambda x: int(x) if pd.notna(x) else 0
    )

    return trials_nodes_df


if __name__ == "__main__":
    ensure_clinical_trials_df(refresh=True)
