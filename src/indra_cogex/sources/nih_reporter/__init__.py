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
        self._core_project_to_application = {}

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
                self._core_project_to_application[
                    row.CORE_PROJECT_NUM
                ] = row.APPLICATION_ID
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
        # Project publications
        for year, publink_file in self.data_files.get("publink").items():
            df = _read_first_df(publink_file)
            for row in df.itertuples():
                application_id = self._core_project_to_application.get(
                    row.PROJECT_NUMBER
                )
                if not application_id:
                    continue
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
                application_id = self._core_project_to_application.get(
                    row["Core Project Number"]
                )
                if not application_id:
                    continue
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
