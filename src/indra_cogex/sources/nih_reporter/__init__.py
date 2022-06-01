"""Processor for the NIH RePORTER data set.

NIH RePORTER is available at https://reporter.nih.gov/. Export for bulk
downloads at: https://reporter.nih.gov/exporter available as zipped csv files per year:
- Projects: table of basic project metadata including activity code,
  various dates, PI code and names, organization info, total cost, etc.
- Publications: table of publications including PMID, PMCID, author lists,
  affiliations etc. but no link to each project. This information is
  usually available via PubMed directly.
- Publication links: table with two columns linking each PMID with a
  project number. The relationship is many-to-many.
- Project abstracts: table with two columns, application ID and corresponding
  abstract.
- Patents: table with patent IDs, titles, linked to project IDs and
  the patent organization name
- Clinical trials: table with core project number, clinical trials ID, study
  name and study status.
"""

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
    "project": re.compile(r"RePORTER_PRJ_C_FY(\d+)\.zip"),
    "publink": re.compile(r"RePORTER_PUBLNK_C_(\d+)\.zip"),
    "abstract": re.compile(r"RePORTER_PRJABS_C_FY(\d+)\.zip"),
    "patent": re.compile(r"Patents_(\d+)\.csv"),
    "clinical_trial": re.compile(r"ClinicalStudies_(\d+)\.csv"),
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
    node_types = ["ResearchProject", "Publication", "ClinicalTrial", "Patent"]

    def __init__(self):
        base_folder = pystow.join("indra", "cogex", "nih_reporter")
        data_files = defaultdict(dict)
        for file_path in base_folder.iterdir():
            for file_type, pattern in fname_regexes.items():
                match = pattern.match(file_path.name)
                if match:
                    data_files[file_type][match.groups()[0]] = file_path
                    break
        self.data_files = dict(data_files)
        self._core_project_applications = defaultdict(list)

    def get_nodes(self) -> Iterable[Node]:
        # Projects
        for year, project_file in self.data_files.get("project").items():
            df = _read_first_df(project_file)
            for _, row in df.iterrows():
                if not pandas.isna(row["SUBPROJECT_ID"]):
                    continue
                data = {
                    pc: row[pc] if not pandas.isna(row[pc]) else None
                    for pc in project_columns
                    # Not all columns are available in all years
                    if pc in row
                }
                yield Node(
                    db_ns="NIHREPORTER.PROJECT",
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
        # Patents
        for _, patent_file in self.data_files.get("patent").items():
            df = pandas.read_csv(patent_file)
            for _, row in df.iterrows():
                yield Node(
                    db_ns="GOOGLE.PATENT",
                    db_id="US%s" % row["PATENT_ID"],
                    data={"name": row["PATENT_TITLE"]},
                    labels=["Patent"],
                )

    def get_relations(self) -> Iterable[Relation]:
        # Project publications
        for year, publink_file in self.data_files.get("publink").items():
            df = _read_first_df(publink_file)
            for _, row in df.iterrows():
                projects = self._core_project_applications.get(
                    row["PROJECT_NUMBER"], []
                )
                for project in projects:
                    yield Relation(
                        source_ns="NIHREPORTER.PROJECT",
                        source_id=project["APPLICATION_ID"],
                        target_ns="PUBMED",
                        target_id=str(row.PMID),
                        rel_type="has_publication",
                    )
        # Project clinical trials
        for _, clinical_trial_file in self.data_files.get("clinical_trial").items():
            df = pandas.read_csv(clinical_trial_file)
            for _, row in df.iterrows():
                projects = self._core_project_applications.get(
                    row["Core Project Number"], []
                )
                for project in projects:
                    yield Relation(
                        source_ns="NIHREPORTER.PROJECT",
                        source_id=project["APPLICATION_ID"],
                        target_ns="CLINICALTRIALS",
                        target_id=row["ClinicalTrials.gov ID"],
                        rel_type="has_clinical_trial",
                    )
        # Project patents
        for _, patent_file in self.data_files.get("patent").items():
            df = pandas.read_csv(patent_file)
            for _, row in df.iterrows():
                projects = self._core_project_applications.get(row["PROJECT_ID"], [])
                for project in projects:
                    yield Relation(
                        source_ns="NIHREPORTER.PROJECT",
                        source_id=project["APPLICATION_ID"],
                        target_ns="GOOGLE.PATENT",
                        target_id="US%s" % row["PATENT_ID"],
                        rel_type="has_patent",
                    )


def _read_first_df(zip_file_path):
    """Extract a single file from the project_file zip file."""
    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        return pandas.read_csv(
            zip_ref.open(zip_ref.filelist[0], "r"), encoding="latin1", low_memory=False
        )
