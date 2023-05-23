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
import logging
import datetime
from typing import Iterable, Any
import zipfile
from collections import defaultdict
import pandas
import pystow
from indra_cogex.sources.processor import Processor
from indra_cogex.representation import Node, Relation


logger = logging.getLogger(__name__)


# Regular expressions to find files of different types
fname_prefixes = {
    "project": "RePORTER_PRJ_C_FY",
    "publink": "RePORTER_PUBLINK_C_",
    "abstract": "RePORTER_PRJABS_C_FY",
    "patent": "Patents_",
    "clinical_trial": "ClinicalStudies_",
}


fname_regexes = {
    "project": re.compile(rf"{fname_prefixes['project']}(\d+)\.zip"),
    "publink": re.compile(rf"{fname_prefixes['publink']}(\d+)\.zip"),
    "abstract": re.compile(rf"{fname_prefixes['abstract']}(\d+)\.zip"),
    "patent": re.compile(rf"{fname_prefixes['patent']}(\d+)\.csv"),
    "clinical_trial": re.compile(rf"{fname_prefixes['clinical_trial']}(\d+)\.csv"),
}

base_url = "https://reporter.nih.gov/exporter"

download_urls = {
    "project": f"{base_url}/projects/download/%s",
    "publink": f"{base_url}/linktables/download/%s",
    "clinical_trial": f"{base_url}/clinicalstudies/download",
    "patent": f"{base_url}/patents/download",
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

    def __init__(self, download=True, force_download=False):
        base_folder = pystow.module("indra", "cogex", "nih_reporter")
        data_files = defaultdict(dict)

        # Download the data files if they are not present
        if download or force_download:
            from datetime import datetime
            last_year = datetime.utcnow().year - 1
            logger.info(
                "Downloading NIH RePORTER data files %s force redownload..."
                % ("with" if force_download else "without")
            )
            download_files(base_folder, force=force_download, last_year=last_year)

        # Collect all the data files
        for file_path in base_folder.base.iterdir():
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
                data = {}
                for pc in project_columns:
                    if pc in row:
                        if "FY" == pc:
                            pc_key = "FY:int"
                        else:
                            pc_key = pc

                        if pandas.isna(row[pc]):
                            data[pc_key] = None
                        else:
                            data[pc_key] = newline_escape(row[pc])
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
        yielded_patents = set()
        for _, patent_file in self.data_files.get("patent").items():
            df = pandas.read_csv(patent_file)
            for _, row in df.iterrows():
                pat_id = row["PATENT_ID"].strip()
                if pat_id and pat_id not in yielded_patents:
                    yield Node(
                        db_ns="GOOGLE.PATENT",
                        db_id="US%s" % row["PATENT_ID"],
                        data={"name": row["PATENT_TITLE"]},
                        labels=["Patent"],
                    )
                    yielded_patents.add(pat_id)

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
    """Extract a single CSV file from a zip file given its path."""
    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        return pandas.read_csv(
            zip_ref.open(zip_ref.filelist[0], "r"),
            encoding="latin1",
            low_memory=False,
            error_bad_lines=False,
        )


def download_files(
    base_folder: pystow.Module, force=False, first_year=1985, last_year=2021
):
    current_year = datetime.date.today().year
    for subset, url_pattern in download_urls.items():
        # These files are indexed by year
        if subset in ["project", "publink"]:
            for year in range(first_year, last_year + 1):
                url = download_urls[subset] % year
                base_folder.ensure(
                    url=url,
                    name=fname_prefixes[subset] + str(year) + ".zip",
                    force=force,
                )
        # These files are single downloads but RePORTER adds a timestamp
        # to the file name making it difficult to check if it already exists
        # so to avoid always redownloading, we take Jan 1st of the current
        # year as reference.
        else:
            timestamp = int(
                datetime.datetime(year=current_year, month=1, day=1).timestamp()
            )
            url = download_urls[subset]
            base_folder.ensure(
                url=url,
                name=fname_prefixes[subset] + str(timestamp) + ".csv",
                force=force,
            )


def newline_escape(text: Any) -> Any:
    """Escape newlines from text"""
    if isinstance(text, str):
        return text.replace("\n", "\\n")
    return text
