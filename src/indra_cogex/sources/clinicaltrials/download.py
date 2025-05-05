"""
Download and parse the ClinicalTrials.gov data using Trialsynth.
"""
import os
from typing import Union

import pystow
import pandas as pd

from indra.ontology.bio import bio_ontology
from trialsynth.clinical_trials_dot_gov import config, process

__all__ = [
    "ensure_clinical_trials_df",
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

    # Process the edges and nodes
    edges_df = process_trialsynth_edges()
    bioentity_nodes_df = process_trialsynth_bioentity_nodes()
    trials_nodes_df = process_trialsynth_trial_nodes()


def _mesh_to_chebi(mesh_curie: Union[str, None]) -> Union[str, None]:
    """Convert a MeSH CURIE to a ChEBI CURIE if possible."""
    if mesh_curie is None:
        return None
    chebi_ns, chebi_id = bio_ontology.map_to(
        ns1="MESH", id1=mesh_curie.split(":")[1], ns2="CHEBI"
    ) or (None, None)
    # The bio_ontology has chebi nodes stored as ("CHEBI", "CHEBI:12345"), CoGEx needs
    # just "chebi:12345"
    return chebi_id.lower() if chebi_id else None


def _nsid_to_name(ns: str, id_: str) -> Union[str, None]:
    """Convert a namespace and ID to a name if possible."""
    return bio_ontology.get_name(ns=ns, id=id_) or None


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
        # has_intervention relation, in CoGEx the edge goes from the bioentity to the trial
        # with a tested_in edge
        "from:CURIE": ":END_ID",
        "to:CURIE": ":START_ID",
        "rel_type:string": ":TYPE",
    }

    # Read the edges file from trialsynth
    edges_df = pd.read_csv(ctconfig.edges_path, sep="\t", compression="gzip")

    # Rename the columns to match CoGEx format
    edges_df.rename(columns=headers_translation, inplace=True)

    # Translate the mesh terms to chebi, since we're only interested in drug trials
    bio_ontology.initialize()
    edges_df[":END_ID"] = edges_df[":END_ID"].apply(_mesh_to_chebi)

    # Drop the "source_registry:string" column
    if "source_registry:string" in edges_df.columns:
        edges_df.drop(columns=["source_registry:string"], inplace=True)

    # Drop rows which didn't map to chebi
    edges_df = edges_df[edges_df[":END_ID"].notna()]

    return edges_df


def process_trialsynth_bioentity_nodes() -> pd.DataFrame:
    """Convert the bioentity nodes file from the trialsynth to CoGEx format

    Returns
    -------
    :
        writeme
    """
    headers_translation = {"curie:CURIE": "id:ID"}

    # Read the bioentity nodes file from trialsynth
    bioentity_nodes_df = pd.read_csv(
        ctconfig.bio_entities_path, sep="\t", compression="gzip"
    )

    # Rename the columns to match CoGEx format
    bioentity_nodes_df.rename(columns=headers_translation, inplace=True)

    # Create a new column for :LABEL
    bioentity_nodes_df[":LABEL"] = "BioEntity"

    # Translate the mesh terms to chebi, since we're only interested in drug trials
    bioentity_nodes_df["id:ID"].apply(_mesh_to_chebi, inplace=True)

    # Drop rows which didn't map to chebi
    bioentity_nodes_df = bioentity_nodes_df[bioentity_nodes_df["id:ID"].notna()]

    # Create a new column for name
    bioentity_nodes_df["name"] = bioentity_nodes_df["id:ID"].apply(
        lambda c: _nsid_to_name(ns="CHEBI", id_=c.upper() if c else None)
    )

    return bioentity_nodes_df[["id:ID", ":LABEL", "name"]]


def process_trialsynth_trial_nodes(df: pd.DataFrame = None) -> pd.DataFrame:
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
    # study_type  # Observational, interventional etc... Fixme: should be among labels but deosn't seem to be there
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
    # start_date:DATE
    # labels:LABEL[]
    # why_stopped:string

    # Translate the headers to CoGEx format, these map 1:1
    headers_translation = {
        "curie:CURIE": "id:ID",
        "why_stopped:string": "why_stopped",
        "status:string": "status",
    }

    def _get_phase(phase_string: str) -> int:
        if pd.notna(phase_string) and phase_string[-1].isdigit():
            return int(phase_string[-1])
        return -1

    # Read the trial nodes file from trialsynth
    if df is None:
        trials_nodes_df = pd.read_csv(ctconfig.trials_path, sep="\t", compression="gzip")
    else:
        trials_nodes_df = df

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
    trials_nodes_df["start_year:int"] = trials_nodes_df["start_date:DATE"].apply(
        lambda x: int(x) if pd.notna(x) else 0
    )

    return trials_nodes_df


if __name__ == "__main__":
    ensure_clinical_trials_df(refresh=True)
