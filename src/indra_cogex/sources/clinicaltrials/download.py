"""
NOTE: ClinicalTrials.gov are working on a more modern API that is currently
in Beta: https://beta.clinicaltrials.gov/data-about-studies/learn-about-api
Once this API is released, we should switch to using it. The instructions for
using the current/old API are below.

Downloading the clinical trials data is now fully automated, but for posterity,
here are the instructions for getting the file manually:

To obtain the custom download for ingest, do the following::

    1. Go to https://clinicaltrials.gov/api/gui/demo/simple_study_fields

    2. Enter the following in the form:

    expr=
    fields=NCTId,BriefTitle,Condition,ConditionMeshTerm,ConditionMeshId,InterventionName,InterventionType,InterventionMeshTerm,InterventionMeshId,StudyType
    min_rnk=1
    max_rnk=500000  # or any number larger than the current number of studies
    fmt=csv

    3. Send Request

    4. Enter the captcha characters into the text box and then press enter
    (make sure to use the enter key and not press any buttons).

    5. The website will display "please wait… " for a couple of minutes, finally,
    the Save to file button will be active.

    6. Click the Save to file button to download the response as a txt file.

    7. Rename the txt file to clinical_trials.csv and then compress it as
    gzip clinical_trials.csv to get clinical_trials.csv.gz, then place
    this file into <pystow home>/indra/cogex/clinicaltrials/
"""

from typing import Optional, List

import pystow
import requests
from tqdm.auto import tqdm, trange
import pandas as pd
import io

__all__ = [
    "CLINICAL_TRIALS_PATH",
    "ensure_clinical_trials",
]

CLINICAL_TRIALS_PATH = pystow.join(
    "indra",
    "cogex",
    "clinicaltrials",
    name="clinical_trials.tsv",
)

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
    "OverallStatus",
    "Phase",
    "WhyStopped",
    "SecondaryIdType",
    "SecondaryId",
    "ReferencePMID",  # these are tagged as relevant by the author, but not necessarily about the trial
]


def ensure_clinical_trials(*, refresh: bool = False) -> pd.DataFrame:
    """Download and parse the ClinicalTrials.gov dataframe.

    If refresh is set to true, it will overwrite the existing file.
    """
    if CLINICAL_TRIALS_PATH.is_file() and not refresh:
        return pd.read_csv(CLINICAL_TRIALS_PATH, sep="\t")
    df = download()
    df.to_csv(CLINICAL_TRIALS_PATH, sep="\t", index=False)
    return df


def download(
    page_size: int = 1_000, fields: Optional[List[str]] = None
) -> pd.DataFrame:
    """Download the ClinicalTrials.gov dataframe.

    If fields is None, will default to :data:`FIELDS`.

    Download takes about 10 minutes and is shown with a progress bar.
    """
    if page_size > 1_000:
        page_size = 1_000
    if fields is None:
        fields = DEFAULT_FIELDS
    base_params = {
        "expr": "",
        "min_rnk": 1,
        "max_rnk": page_size,
        "fmt": "csv",
        "fields": ",".join(fields),
    }
    url = "https://classic.clinicaltrials.gov/api/query/study_fields"

    #: This is the number of dummy rows at the beginning of the document
    #: before the actual CSV starts
    skiprows = 9

    beginning = '"NStudiesAvail: '
    res = requests.get(url, params=base_params)
    for line in res.text.splitlines()[:skiprows]:
        if line.startswith(beginning):
            total = int(line.removeprefix(beginning).strip('"'))
            break
    else:
        raise ValueError("could not parse total trials")

    pages = 1 + total // page_size

    tqdm.write(
        f"There are {total:,} clinical trials available, iterable in {pages:,} pages of size {page_size:,}."
    )

    first_page_df = pd.read_csv(io.StringIO(res.text), skiprows=skiprows)

    dfs = [first_page_df]

    # start on page "1" because we already did page 0 above. Note that we're zero-indexed,
    # so "1" is actually is the second page
    for page in trange(1, pages, unit="page", desc="Downloading ClinicalTrials.gov"):
        min_rnk = page_size * page + 1
        max_rnk = page_size * (page + 1)
        res = requests.get(
            url, params={**base_params, "min_rnk": min_rnk, "max_rnk": max_rnk}
        )
        page_df = pd.read_csv(io.StringIO(res.text), skiprows=skiprows)
        dfs.append(page_df)

    return pd.concat(dfs)


if __name__ == "__main__":
    ensure_clinical_trials(refresh=True)
