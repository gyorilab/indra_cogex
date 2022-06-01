import re
from typing import Iterable
import zipfile
from collections import defaultdict
import pandas
import pystow
from indra_cogex.sources.processor import Processor
from indra_cogex.representation import Node, Relation


# Regular expressions to find files of different types
fname_regexes = {
    "project": re.compile(r"RePORTER_PRJ_C_FY(\d+).zip"),
    "publink": re.compile(r"RePORTER_PUBLNK_C_(\d+).zip"),
    "abstract": re.compile(r"RePORTER_PRJABS_C_FY(\d+).zip"),
    "patent": re.compile(r"Patents_(\d+).csv"),
    "clinical_trial": re.compile(r"ClinicalStudies_(\d+).csv"),
}


# Project columns to include as node attributes, note that not all columns
# are included here.
project_columns = [
    "ACTIVITY",
    "ADMINISTERING_IC",
    "CORE_PROJECT_NUM",
    "FY",
    "ORG_NAME",
    "PI_IDS",
    "PI_NAMEs",
    "DIRECT_COST_AMT",
    "PROJECT_TITLE",
    "SUBPROJECT_ID" "FULL_PROJECT_ID",
]


class NihReporterProcessor(Processor):
    """Processor for NIH Reporter database."""

    name = "nih_reporter"
    node_types = ["ResearchProject", "Publication", "ClinicalTrial"]

    def __init__(self):
        base_folder = pystow.join("indra", "cogex", "nih_reporter")
        data_files = defaultdict(dict)
        for file_path in base_folder.iterdir():
            for file_type, pattern in fname_regexes.items():
                match = pattern.match(file_path.name)
                if match:
                    data_files[file_type][match.groups()[0]] = file_path
        self.data_files = dict(data_files)
        self._core_project_applications = defaultdict(list)

    def get_nodes(self) -> Iterable[Node]:
        # Projects
        for year, project_file in self.data_files.get("project").items():
            df = _read_first_df(project_file)
            for _, row in df.iterrows():
                data = {
                    pc: row[pc]
                    for pc in project_columns
                    # Not all columns are available in all years
                    if pc in row
                }
                yield Node(
                    db_ns="NIHREPORTER",
                    db_id=row.APPLICATION_ID,
                    labels=["ResearchProject"],
                    data=data,
                )
                self._core_project_applications[row.CORE_PROJECT_NUM].append(dict(row))
        # Publications
        for year, publink_file in self.data_files.get("publink").items():
            df = _read_first_df(publink_file)
            for row in df.itertuples():
                yield Node(
                    db_ns="PUBMED",
                    db_id=str(row.PMID),
                    labels=["Publication"],
                )
        # Clinical trials
        for _, clinical_trial_file in self.data_files.get("clinical_trial").items():
            df = pandas.read_csv(clinical_trial_file)
            for _, row in df.iterrows():
                yield Node(
                    db_ns="CLINICALTRIALS",
                    db_id=row["ClinicalTrials.gov ID"],
                    labels=["ClinicalTrial"],
                )
        # NOTE: we don't process patents for now

    def get_relations(self) -> Iterable[Relation]:
        self.unmapped_projects = {}
        mappings_to_prime = {}
        # Internal project relations
        for (
            core_project_number,
            applications,
        ) in self._core_project_applications.items():
            internal_relations, prime_project = get_internal_project_relations(
                applications
            )
            for sub_project, parent_project in internal_relations.items():
                if not parent_project:
                    self.unmapped_projects[sub_project] = core_project_number
                    continue
                yield Relation(
                    source_ns="NIHREPORTER",
                    source_id=sub_project,
                    target_ns="NIHREPORTER",
                    target_id=parent_project,
                    rel_type="part_of",
                )
            mappings_to_prime[core_project_number] = prime_project["APPLICATION_ID"]

        # Project publications
        for year, publink_file in self.data_files.get("publink").items():
            df = _read_first_df(publink_file)
            for row in df.itertuples():
                application_id = mappings_to_prime.get[row["PROJECT_NUMBER"]]
                yield Relation(
                    source_ns="NIHREPORTER",
                    source_id=application_id,
                    target_ns="PUBMED",
                    target_id=str(row.PMID),
                    rel_type="has_publication",
                )
        # Project clinical trials
        for _, clinical_trial_file in self.data_files.get("clinical_trial").items():
            df = pandas.read_csv(clinical_trial_file)
            for _, row in df.iterrows():
                application_id = mappings_to_prime[row["Core Project Number"]]
                yield Relation(
                    source_ns="NIHREPORTER",
                    source_id=application_id,
                    target_ns="CLINICALTRIALS",
                    target_id=row["ClinicalTrials.gov ID"],
                    rel_type="has_clinical_trial",
                )


def _read_first_df(zip_file_path):
    # extract a single file from the project_file zip file
    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        return pandas.read_csv(
            zip_ref.open(zip_ref.filelist[0], "r"), encoding="latin1", low_memory=False
        )


def get_internal_project_relations(core_project_applications):
    sub_projects = [
        p for p in core_project_applications if not pandas.isna(p["SUBPROJECT_ID"])
    ]
    non_sub_projects = [
        p for p in core_project_applications if pandas.isna(p["SUBPROJECT_ID"])
    ]
    relations = {}
    for sp in sub_projects:
        if len(non_sub_projects) > 1:
            possible_parents = [
                p
                for p in non_sub_projects
                if p["FULL_PROJECT_NUM"] == sp["FULL_PROJECT_NUM"]
            ]
            if not possible_parents:
                possible_parents = [
                    p for p in non_sub_projects if p["FOA_NUMBER"] == sp["FOA_NUMBER"]
                ]
            if len(possible_parents) != 1:
                possible_parents = [None]
        else:
            possible_parents = [non_sub_projects[0]]
        relations[sp["APPLICATION_ID"]] = (
            possible_parents[0]["APPLICATION_ID"] if possible_parents[0] else None
        )

    prime_project = sorted(
        non_sub_projects,
        key=lambda x: len([v for v in relations.values() if v == x["APPLICATION_ID"]]),
        reverse=True,
    )[0]
    return relations, prime_project


def _get_parent_project(this_project, projects_in_core):
    if len(projects_in_core) == 1:
        return this_project["APPLICATION_ID"]
    possible_parents = [
        p
        for p in projects_in_core
        if p["FULL_PROJECT_NUM"] == this_project["FULL_PROJECT_NUM"]
    ]
    possible_parents = [p for p in possible_parents if pandas.isna(p["SUBPROJECT_ID"])]
    if len(possible_parents) != 1:
        print(this_project)
    return possible_parents[0]["APPLICATION_ID"]
