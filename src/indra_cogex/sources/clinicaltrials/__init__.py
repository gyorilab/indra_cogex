"""This module implements input for ClinicalTrials.gov data."""

import logging

import pandas as pd
import tqdm

from indra.ontology.bio import bio_ontology
from indra_cogex.client import process_identifier
from indra_cogex.sources.processor import Processor
from indra_cogex.representation import Node, Relation
from indra_cogex.sources.utils import get_bool
from indra_cogex.sources.clinicaltrials.download import (
    ensure_clinical_trials_df,
    process_trialsynth_edges,
    process_trialsynth_trial_publication_edges,
    process_trialsynth_bioentity_nodes,
    process_trialsynth_trial_nodes,
    load_all,
)


logger = logging.getLogger(__name__)


class ClinicaltrialsProcessor(Processor):
    name = "clinicaltrials"
    node_types = ["BioEntity", "ClinicalTrial"]

    def __init__(
        self,
        reprocess: bool = False,
        redownload: bool = False,
        max_pages: int = None,  # Primarily used for testing
    ):
        ensure_clinical_trials_df(
            reprocess=reprocess,
            redownload=redownload,
            max_pages=max_pages
        )

        self.trials_df = process_trialsynth_trial_nodes()
        # Warm up bio ontology
        _ = bio_ontology.get_name("HGNC", "1100")
        self.edges_df = process_trialsynth_edges()
        self.publication_trial_edges_df = process_trialsynth_trial_publication_edges()
        self.mesh_chebi_map = {
            old_id: new_id for new_id, old_id in
            self.edges_df[["bioentity_mapped", "bioentity"]].values
            if new_id.lower().startswith("chebi:") and old_id.lower().startswith("mesh:")
        }
        self.bioentities_df = process_trialsynth_bioentity_nodes(self.mesh_chebi_map)

    def get_nodes(self):
        yield from self._get_trial_bioentity_nodes()
        yield from self._get_publication_nodes()

    def _get_publication_nodes(self):
        for pmid in tqdm.tqdm(
            self.publication_trial_edges_df["pmid"].unique(),
            total=len(self.publication_trial_edges_df),
            desc="Publication nodes",
        ):
            yield Node(
                db_ns="PUBMED",
                db_id=str(pmid),
                labels=["Publication"],
                data={},
            )

    def _get_trial_bioentity_nodes(self):
        yielded_nodes = set()
        for ix, row in tqdm.tqdm(
            self.trials_df.iterrows(), total=len(self.trials_df), desc="Trial nodes"
        ):
            nctid = row["id:ID"]
            if nctid in yielded_nodes:
                continue
            yielded_nodes.add(nctid)
            db_ns, db_id = process_identifier(nctid)
            yield Node(
                db_ns=db_ns,
                db_id=db_id,
                labels=["ClinicalTrial"],
                data={
                    "study_type": or_na(row["study_type"]),
                    "randomized:boolean": get_bool(row["randomized:boolean"]),
                    "status": or_na(row["status"]),
                    "phase:int": row["phase:int"],
                    "why_stopped": or_na(row["why_stopped"]),
                    "start_year:int": or_na(row["start_year"]),
                    "start_year_anticipated:boolean": get_bool(
                        row["start_year_anticipated:boolean"]
                    ),
                    "completion_year:int": or_na(row["completion_year"]),
                    "completion_year_anticipated:boolean": get_bool(
                        row["completion_year_type:string"].lower() == "estimated"
                    ) if pd.notna(row["completion_year_type:string"]) else None,
                    "last_update_year:int": or_na(
                        row["last_update_submit_year:integer"]
                    ),
                },
            )

        for ix, row in tqdm.tqdm(
            self.bioentities_df.iterrows(), total=len(self.bioentities_df), desc="BioEntity nodes"
        ):
            bioentity = row["bioentity_mapped"]
            if bioentity in yielded_nodes:
                continue
            yielded_nodes.add(bioentity)
            db_ns, db_id = process_identifier(bioentity)
            yield Node(
                db_ns=db_ns,
                db_id=db_id,
                labels=["BioEntity"],
                data={"name": row["name"]},
            )

    def get_relations(self):
        yield from self._get_condition_intervention_relations()
        yield from self._get_publication_trial_relations()

    def _get_publication_trial_relations(self):
        for ix, (trial_curie, pmid, _, source, ref_type) in tqdm.tqdm(
            self.publication_trial_edges_df.iterrows(), total=len(self.publication_trial_edges_df), desc="Publication-Trial edges"
        ):
            trial_ns, trial_id = process_identifier(trial_curie)

            yield Relation(
                source_ns=trial_ns,
                source_id=trial_id,
                target_ns="PUBMED",
                target_id=str(pmid),
                rel_type="has_publication",
                data={
                    # ref_type is only available for relations sourced from
                    # clinicaltrials.gov; see reference processing in the
                    # trialsynth.ctgov.fetch module. For relations sourced from
                    # PubMed, ref_type is None.
                    "ref_type": or_na(ref_type),
                    "source": source,
                },
            )

    def _get_condition_intervention_relations(self):
        added = set()
        rel_translation = {
            "has_condition": "has_trial",
            "has_intervention": "tested_in",
        }
        for ix, row in tqdm.tqdm(
            self.edges_df.iterrows(), total=len(self.edges_df), desc="Edges"
        ):
            # Conditions: use "has_trial" relation going to the trial from the condition
            # Interventions: use "tested_in" relation going to the trial from the intervention
            # The Trialsynth edges go from the trial to the bioentity with a
            # has_intervention or has_condition relation. In CoGEx the edge goes
            # from the bioentity to the trial with a tested_in or has_trial edge

            bioentity = row["bioentity_mapped"]
            rel_type = rel_translation.get(row["rel_type:string"])
            if rel_type is None:
                raise ValueError(f"Unknown relation type: {row['rel_type:string']}")

            nctid_curie = row["trial"]
            if (bioentity, nctid_curie, rel_type) in added:
                continue

            db_ns, db_id = process_identifier(bioentity)
            trial_ns, trial_id = process_identifier(nctid_curie)
            yield Relation(
                source_ns=db_ns,
                source_id=db_id,
                target_ns=trial_ns,
                target_id=trial_id,
                rel_type=rel_type,
                data={
                    "ctgov:boolean": get_bool(
                        "mesh" in row["grounding_sources:string[]"]
                    ),
                    "gilda:boolean": get_bool(
                        "gilda" in row["grounding_sources:string[]"]
                    ),
                },
            )
            added.add((bioentity, nctid_curie, rel_type))


def or_na(x):
    """Return None if x is NaN, otherwise return x"""
    return None if pd.isna(x) else x


class ClinicalTrialResultProcessor(Processor):
    """Processor for clinical trial result nodes extracted from publications.

    Reads GPT-extracted grounded JSONs and emits 7 node types and 9 relation
    types covering trial arms, metrics, adverse events, criteria, outcomes,
    statistical comparisons, and genetic markers.
    """

    name = "clinical_trial_results"
    node_types = [
        "TrialResult",
        "TrialArm",
        "TrialMetric",
        "TrialAdverseEvent",
        "TrialCriterion",
        "TrialOutcome",
        "TrialStatisticalComparison",
    ]

    def __init__(self):
        data = load_all()
        self.result_nodes_df = data["result_nodes"]
        self.arms_df = data["arms"]
        self.metrics_df = data["metrics"]
        self.adverse_events_df = data["adverse_events"]
        self.criteria_df = data["criteria"]
        self.outcomes_df = data["outcomes"]
        self.stat_comparisons_df = data["stat_comparisons"]
        self.genetic_edges_df = data["genetic_edges"]
        self.ae_bioentity_edges_df = data["ae_bioentity_edges"]
        self.publication_edges_df = data["publication_edges"]

    def get_nodes(self):
        for _, row in tqdm.tqdm(self.result_nodes_df.iterrows(),
                                total=len(self.result_nodes_df),
                                desc="TrialResult nodes"):
            yield Node(
                db_ns="trial.result", db_id=str(row["result_id"]),
                labels=["TrialResult"],
                data={
                    "study_info": row["study_info"],
                    "trial_ids:string[]": row["trial_ids:string[]"],
                    "phase": row["phase"],
                    "locations:string[]": row["locations:string[]"],
                },
            )

        for _, row in tqdm.tqdm(self.arms_df.iterrows(),
                                total=len(self.arms_df),
                                desc="TrialArm nodes"):
            yield Node(
                db_ns="trial.arm", db_id=str(row["arm_id"]),
                labels=["TrialArm"],
                data={
                    "arm_name": row["arm_name"],
                    "n:int": "" if pd.isna(row["n"]) else int(row["n"]),
                    "dosage": row["dosage"],
                    "source_sentence": row["source_sentence"],
                },
            )

        for _, row in tqdm.tqdm(self.metrics_df.iterrows(),
                                total=len(self.metrics_df),
                                desc="TrialMetric nodes"):
            yield Node(
                db_ns="trial.metric", db_id=str(row["metric_id"]),
                labels=["TrialMetric"],
                data={
                    "name": row["name"],
                    "value_numeric:float": "" if pd.isna(row["value_numeric"]) else str(row["value_numeric"]),
                    "unit": row["unit"],
                    "value_text": row["value_text"],
                    "source_sentence": row["source_sentence"],
                },
            )

        for _, row in tqdm.tqdm(self.adverse_events_df.iterrows(),
                                total=len(self.adverse_events_df),
                                desc="TrialAdverseEvent nodes"):
            yield Node(
                db_ns="trial.adverseevent", db_id=str(row["adverseevent_id"]),
                labels=["TrialAdverseEvent"],
                data={
                    "event_name": row["event_name"],
                    "incidence_numeric:float": "" if pd.isna(row["incidence_numeric"]) else str(row["incidence_numeric"]),
                    "unit": row["unit"],
                    "value_text": row["value_text"],
                    "source_sentence": row["source_sentence"],
                },
            )

        for _, row in tqdm.tqdm(self.criteria_df.iterrows(),
                                total=len(self.criteria_df),
                                desc="TrialCriterion nodes"):
            yield Node(
                db_ns="trial.criterion", db_id=str(row["criterion_id"]),
                labels=["TrialCriterion"],
                data={
                    "text": row["text"],
                    "criterion_type": row["criterion_type"],
                    "evidence_text": row["evidence_text"],
                },
            )

        for _, row in tqdm.tqdm(self.outcomes_df.iterrows(),
                                total=len(self.outcomes_df),
                                desc="TrialOutcome nodes"):
            yield Node(
                db_ns="trial.outcome", db_id=str(row["outcome_id"]),
                labels=["TrialOutcome"],
                data={
                    "text": row["text"],
                    "evidence_text": row["evidence_text"],
                },
            )

        for _, row in tqdm.tqdm(self.stat_comparisons_df.iterrows(),
                                total=len(self.stat_comparisons_df),
                                desc="TrialStatisticalComparison nodes"):
            yield Node(
                db_ns="trial.statcomparison", db_id=str(row["statcomparison_id"]),
                labels=["TrialStatisticalComparison"],
                data={"comparison_name": row["comparison_name"]},
            )

    def get_relations(self):
        for _, row in tqdm.tqdm(self.publication_edges_df.iterrows(),
                                total=len(self.publication_edges_df),
                                desc="Publication->TrialResult"):
            yield Relation(
                source_ns="PUBMED", source_id=row["pmid"],
                target_ns="trial.result", target_id=str(row["result_id"]),
                rel_type="has_trial_result",
            )

        for _, row in tqdm.tqdm(self.arms_df.iterrows(),
                                total=len(self.arms_df),
                                desc="TrialResult->TrialArm"):
            yield Relation(
                source_ns="trial.result", source_id=str(row["result_id"]),
                target_ns="trial.arm", target_id=str(row["arm_id"]),
                rel_type="has_arm",
            )

        for _, row in tqdm.tqdm(self.metrics_df.iterrows(),
                                total=len(self.metrics_df),
                                desc="TrialArm/TrialComparison->TrialMetric"):
            parent_ns = "trial.arm" if row["parent_ns"] == "arm" else "trial.statcomparison"
            yield Relation(
                source_ns=parent_ns, source_id=str(row["parent_id"]),
                target_ns="trial.metric", target_id=str(row["metric_id"]),
                rel_type="has_metric",
            )

        for _, row in tqdm.tqdm(self.adverse_events_df.iterrows(),
                                total=len(self.adverse_events_df),
                                desc="TrialArm->TrialAdverseEvent"):
            yield Relation(
                source_ns="trial.arm", source_id=str(row["arm_id"]),
                target_ns="trial.adverseevent", target_id=str(row["adverseevent_id"]),
                rel_type="has_adverse_event",
            )

        for _, row in tqdm.tqdm(self.criteria_df.iterrows(),
                                total=len(self.criteria_df),
                                desc="TrialResult->TrialCriterion"):
            rel_type = (
                "has_inclusion_criterion"
                if row["criterion_type"] == "inclusion"
                else "has_exclusion_criterion"
            )
            yield Relation(
                source_ns="trial.result", source_id=str(row["result_id"]),
                target_ns="trial.criterion", target_id=str(row["criterion_id"]),
                rel_type=rel_type,
            )

        for _, row in tqdm.tqdm(self.outcomes_df.iterrows(),
                                total=len(self.outcomes_df),
                                desc="TrialResult->TrialOutcome"):
            yield Relation(
                source_ns="trial.result", source_id=str(row["result_id"]),
                target_ns="trial.outcome", target_id=str(row["outcome_id"]),
                rel_type="has_outcome",
            )

        for _, row in tqdm.tqdm(self.stat_comparisons_df.iterrows(),
                                total=len(self.stat_comparisons_df),
                                desc="TrialResult->TrialStatisticalComparison"):
            yield Relation(
                source_ns="trial.result", source_id=str(row["result_id"]),
                target_ns="trial.statcomparison", target_id=str(row["statcomparison_id"]),
                rel_type="has_statistical_comparison",
            )

        for _, row in tqdm.tqdm(self.genetic_edges_df.iterrows(),
                                total=len(self.genetic_edges_df),
                                desc="TrialResult->Gene"):
            yield Relation(
                source_ns="trial.result", source_id=str(row["result_id"]),
                target_ns="HGNC", target_id=row["hgnc_id"],
                rel_type="has_genetic_criterion",
            )

        for _, row in tqdm.tqdm(self.ae_bioentity_edges_df.iterrows(),
                                total=len(self.ae_bioentity_edges_df),
                                desc="TrialAdverseEvent->BioEntity"):
            yield Relation(
                source_ns="trial.adverseevent", source_id=str(row["adverseevent_id"]),
                target_ns=row["db"], target_id=row["id"],
                rel_type="adverse_event_grounded_as",
            )

        for _, row in tqdm.tqdm(self.result_nodes_df.iterrows(),
                                total=len(self.result_nodes_df),
                                desc="TrialResult->ClinicalTrial"):
            trial_ids_raw = row.get("trial_ids:string[]", "")
            if pd.isna(trial_ids_raw) or not trial_ids_raw:
                continue
            for nct_id in str(trial_ids_raw).split(";"):
                nct_id = nct_id.strip()
                if nct_id and nct_id.upper().startswith("NCT"):
                    yield Relation(
                        source_ns="trial.result", source_id=str(row["result_id"]),
                        target_ns="clinicaltrials", target_id=nct_id,
                        rel_type="has_trial_source",
                    )
