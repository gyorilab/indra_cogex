"""
Download and parse the ClinicalTrials.gov data using Trialsynth.
"""
import json
import logging
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pystow
import pandas as pd

from indra.ontology.bio import bio_ontology
from indra_cogex.client import process_identifier
from indra_cogex.representation import dump_norm_id
from trialsynth.ctgov import config, process

__all__ = [
    "ensure_clinical_trials_df",
    "process_trialsynth_edges",
    "process_trialsynth_bioentity_nodes",
    "process_trialsynth_trial_nodes",
    "load_all",
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
        reprocess the raw data, but not redownload it if it already exists.
        Default: False.
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
            ctconfig.trial_publication_edges_path,
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


def _normalize_bioentity_curie(curie: str) -> str:
    if pd.isna(curie):
        return curie
    db_ns, db_id = process_identifier(curie)
    return dump_norm_id(db_ns, db_id)


def _merge_grounding_sources(sources: pd.Series) -> str:
    merged: set[str] = set()
    for value in sources:
        if pd.isna(value) or value == "":
            continue
        merged.update(
            item.strip() for item in str(value).split(";") if item.strip()
        )
    return ";".join(sorted(merged))


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

    # Normalize bioentity CURIEs so equivalent identifiers group together
    edges_df["bioentity_mapped"] = edges_df["bioentity_mapped"].map(
        _normalize_bioentity_curie
    )

    # Merge duplicate rows on trial, mapped bioentity, and relation type while
    # unioning semicolon-separated grounding source lists
    merge_keys = ["trial", "bioentity_mapped", "rel_type:string"]
    agg_columns = {
        col: "first"
        for col in edges_df.columns
        if col not in merge_keys + ["grounding_sources:string[]"]
    }
    agg_columns["grounding_sources:string[]"] = _merge_grounding_sources
    edges_df = edges_df.groupby(merge_keys, as_index=False).agg(agg_columns)

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

    # Clean up
    # Remove newlines and 2+ whitespaces from the why_stopped column
    trials_nodes_df["why_stopped"] = trials_nodes_df["why_stopped"].str.replace(
        r"\s+", " ", regex=True
    ).str.strip()

    return trials_nodes_df


#: Path to the directory containing GPT-extracted grounded JSON files.
JSON_DIR = pystow.join("indra", "cogex", "clinical_trial_results", "grounded")


def _load_jsons(json_dir: Path = JSON_DIR) -> List[Tuple[int, str, dict]]:
    """Load all grounded JSON files, returning (result_id, pmid, data) tuples.

    Parameters
    ----------
    json_dir :
        Path to directory containing grounded JSON files.

    Returns
    -------
    :
        List of (result_id, pmid, data) tuples, one per JSON file.
    """
    records = []
    for result_id, path in enumerate(sorted(json_dir.glob("*.json")), start=1):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            pmid = str(data.get("pmid", path.stem))
            records.append((result_id, pmid, data))
        except Exception as e:
            logger.warning("Failed to load %s: %s", path.name, e)
    logger.info("Loaded %d JSON files from %s", len(records), json_dir)
    return records


def load_all(json_dir: Path = JSON_DIR) -> Dict[str, pd.DataFrame]:
    """Load all grounded JSONs in a single pass and return all DataFrames.

    Parameters
    ----------
    json_dir :
        Path to the directory containing grounded JSON files.

    Returns
    -------
    :
        Dictionary with keys: result_nodes, arms, metrics, adverse_events,
        criteria, outcomes, stat_comparisons, genetic_edges, ae_bioentity_edges,
        publication_edges.
    """
    result_nodes = []
    arms = []
    metrics = []
    adverse_events = []
    criteria = []
    outcomes = []
    stat_comparisons = []
    genetic_edges = []
    ae_bioentity_edges = []
    publication_edges = []

    arm_id = 1
    metric_id = 1
    ae_id = 1
    criterion_id = 1
    outcome_id = 1
    stat_id = 1

    for result_id, pmid, data in _load_jsons(json_dir):
        result_nodes.append({
            "result_id": result_id,
            "study_info": data.get("study_info", ""),
            "trial_ids:string[]": ";".join(data.get("trial_ids", [])),
            "phase": data.get("phase") or "",
            "locations:string[]": ";".join(data.get("locations", [])),
        })
        publication_edges.append({"pmid": pmid, "result_id": result_id})

        for arm in data.get("arms", []):
            arms.append({
                "result_id": result_id,
                "arm_id": arm_id,
                "arm_name": arm.get("arm_name", ""),
                "n": arm.get("n"),
                "dosage": arm.get("dosage") or "",
                "source_sentence": arm.get("source_sentence", ""),
            })
            for m in arm.get("metrics", []):
                metrics.append({
                    "parent_ns": "arm",
                    "parent_id": arm_id,
                    "metric_id": metric_id,
                    "name": m.get("name", ""),
                    "value_numeric": m.get("value_numeric"),
                    "unit": m.get("unit", ""),
                    "value_text": m.get("value_text", ""),
                    "source_sentence": m.get("source_sentence", ""),
                })
                metric_id += 1
            for ae in arm.get("adverse_events", []):
                adverse_events.append({
                    "arm_id": arm_id,
                    "adverseevent_id": ae_id,
                    "event_name": ae.get("event_name", ""),
                    "incidence_numeric": ae.get("incidence_numeric"),
                    "unit": ae.get("unit", ""),
                    "value_text": ae.get("value_text", ""),
                    "source_sentence": ae.get("source_sentence", ""),
                })
                grounding = ae.get("grounding") or {}
                if grounding.get("db") and grounding.get("id"):
                    ae_bioentity_edges.append({
                        "adverseevent_id": ae_id,
                        "db": grounding["db"],
                        "id": grounding["id"],
                    })
                ae_id += 1
            arm_id += 1

        for item in data.get("inclusion_criteria", []):
            criteria.append({
                "result_id": result_id,
                "criterion_id": criterion_id,
                "text": item.get("text", ""),
                "criterion_type": "inclusion",
                "evidence_text": item.get("evidence_text", ""),
            })
            criterion_id += 1

        for item in data.get("exclusion_criteria", []):
            criteria.append({
                "result_id": result_id,
                "criterion_id": criterion_id,
                "text": item.get("text", ""),
                "criterion_type": "exclusion",
                "evidence_text": item.get("evidence_text", ""),
            })
            criterion_id += 1

        for item in data.get("results", []):
            outcomes.append({
                "result_id": result_id,
                "outcome_id": outcome_id,
                "text": item.get("text", ""),
                "evidence_text": item.get("evidence_text", ""),
            })
            outcome_id += 1

        for comp in data.get("statistical_comparisons", []):
            stat_comparisons.append({
                "result_id": result_id,
                "statcomparison_id": stat_id,
                "comparison_name": comp.get("comparison_name", ""),
            })
            for m in comp.get("metrics", []):
                metrics.append({
                    "parent_ns": "statcomparison",
                    "parent_id": stat_id,
                    "metric_id": metric_id,
                    "name": m.get("name", ""),
                    "value_numeric": m.get("value_numeric"),
                    "unit": m.get("unit", ""),
                    "value_text": m.get("value_text", ""),
                    "source_sentence": m.get("source_sentence", ""),
                })
                metric_id += 1
            stat_id += 1

        grounded = data.get("genetic", {}).get("grounded_inclusion", [])
        for entry in grounded:
            for grounding in entry.get("groundings", []):
                info = grounding.get("info", {})
                if info.get("db") == "HGNC" and info.get("id"):
                    genetic_edges.append({
                        "result_id": result_id,
                        "hgnc_id": info["id"],
                        "symbol": info.get("entry_name", grounding.get("symbol", "")),
                        "variant": entry.get("variant"),
                    })

    logger.info("Extracted %d TrialArm nodes", len(arms))
    logger.info("Extracted %d TrialMetric nodes", len(metrics))
    logger.info("Extracted %d TrialAdverseEvent nodes", len(adverse_events))
    logger.info("Extracted %d TrialCriterion nodes", len(criteria))
    logger.info("Extracted %d TrialOutcome nodes", len(outcomes))
    logger.info("Extracted %d TrialStatisticalComparison nodes", len(stat_comparisons))
    logger.info("Extracted %d has_genetic_criterion edges", len(genetic_edges))
    logger.info("Extracted %d adverse_event_grounded_as edges", len(ae_bioentity_edges))

    genetic_edges_df = pd.DataFrame(genetic_edges)
    if not genetic_edges_df.empty:
        genetic_edges_df = genetic_edges_df.drop_duplicates(subset=["result_id", "hgnc_id"])
        logger.info(
            "%d unique has_genetic_criterion edges after deduplication",
            len(genetic_edges_df),
        )
    ae_bioentity_edges_df = pd.DataFrame(ae_bioentity_edges)
    if not ae_bioentity_edges_df.empty:
        ae_bioentity_edges_df = ae_bioentity_edges_df.drop_duplicates(
            subset=["adverseevent_id", "db", "id"]
        )
        logger.info(
            "%d unique adverse_event_grounded_as edges after deduplication",
            len(ae_bioentity_edges_df),
        )

    return {
        "result_nodes": pd.DataFrame(result_nodes),
        "arms": pd.DataFrame(arms),
        "metrics": pd.DataFrame(metrics),
        "adverse_events": pd.DataFrame(adverse_events),
        "criteria": pd.DataFrame(criteria),
        "outcomes": pd.DataFrame(outcomes),
        "stat_comparisons": pd.DataFrame(stat_comparisons),
        "genetic_edges": genetic_edges_df,
        "ae_bioentity_edges": ae_bioentity_edges_df,
        "publication_edges": pd.DataFrame(publication_edges),
    }
