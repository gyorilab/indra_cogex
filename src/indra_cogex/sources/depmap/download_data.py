import datetime
import logging
from functools import lru_cache

import requests
from tqdm import tqdm

from indra_cogex.sources.depmap.constants import SUBMODULE


# DepMap API
URL = "https://depmap.org/portal/download/api/downloads"


@lru_cache(1)
def get_downloads_table():
    """Get the full downloads table from the DepMap API."""
    return requests.get(URL).json()


@lru_cache(1)
def get_latest_depmap() -> str:
    """Get the latest release name."""
    latest = next(
        release for release in get_downloads_table()["releaseData"] if release["isLatest"]
    )
    return latest["releaseName"]


def get_download_url(name: str, release_name: str = None) -> str:
    """Get the download URL for a given file name and version.

    Parameters
    ----------
    name :
        The name of the file to download.
    release_name :
        The name of the release to download from. If None, the latest release is used.
        If provided, the release name must match the one in the downloads table.

    Returns
    -------
    :
        The download URL for the specified file.
    """
    files = []
    for download in get_downloads_table()["table"]:
        if download["fileName"] == name:
            if release_name is not None and download["releaseName"] != release_name:
                continue
            files.append(
                (
                    download["downloadUrl"],
                    datetime.datetime.strptime(download["date"], "%m/%y"),
                )
            )
    if not files:
        raise ValueError(f"Could not find {name} in downloads table")
    # Sort by date, most recent first
    files.sort(key=lambda x: x[1], reverse=True)
    # Return the most recent file
    return files[0][0]


# The rest
MITOCARTA_NAME = "Human.MitoCarta3.0.xls"  # Rarely updates, see https://www.broadinstitute.org/mitocarta
MODEL_INFO_NAME = "Model.csv"  # To get mapping from model name to CCLE Name
CRISPR_NAME = "CRISPRGeneEffect.csv"  # CRISPr data
RNAI_NAME = "D2_combined_gene_dep_scores.csv"  # RNAi data
DEPMAP_RELEASE = get_latest_depmap().split()[-1].lower()  # e.g., "21q4"
DEPMAP_RELEASE_MODULE = SUBMODULE.module(DEPMAP_RELEASE)


# http://www.broadinstitute.org/ftp/distribution/metabolic/papers/Pagliarini/MitoCarta3.0/Human.MitoCarta3.0.xls
# OR https://personal.broadinstitute.org/scalvo/MitoCarta3.0/Human.MitoCarta3.0.xls
MITOCARTA_FILE = SUBMODULE.join(name=MITOCARTA_NAME)  # Rarely updates
MITOCARTA_URL = "https://personal.broadinstitute.org/scalvo/MitoCarta3.0/Human.MitoCarta3.0.xls"
MODEL_INFO_FILE = DEPMAP_RELEASE_MODULE.join(name=MODEL_INFO_NAME)
RNAI_FILE = SUBMODULE.join(name=RNAI_NAME)  # Rarely updates
CRISPR_FILE = DEPMAP_RELEASE_MODULE.join(name=CRISPR_NAME)


logger = logging.getLogger(__name__)


def download_pbar(url: str, fname: str):
    """Download a file with a tqdm progress bar

    Parameters
    ----------
    url :
        The URL to download the file from.
    fname :
        Where to save the file to.
    """
    # From https://stackoverflow.com/a/62113293/10478812
    resp = requests.get(url, stream=True)
    total = int(resp.headers.get('content-length', 0))
    with open(fname, 'wb') as file, tqdm(
        desc=fname.split('/')[-1],
        total=total,
        unit='iB',
        unit_scale=True,
        unit_divisor=1024,
    ) as bar:
        for data in resp.iter_content(chunk_size=1024):
            size = file.write(data)
            bar.update(size)


def get_latest_rnai_url():
    """Get the latest RNAi file URL."""
    table = get_downloads_table()["table"]
    for entry in table:
        if entry["fileName"] == RNAI_NAME:
            return entry["downloadUrl"]
    raise ValueError(f"Could not find {RNAI_NAME} in downloads table")


def download_source_files(force: bool = False):
    """Ensure the source files are downloaded

    Parameters
    ----------
    force :
        If True, force the download of the source files.

    Returns
    -------

    """
    # Get DepMap source files
    latest_achilles_file_url = get_latest_rnai_url()
    if force or not RNAI_FILE.exists():
        download_pbar(latest_achilles_file_url, str(RNAI_FILE))
    else:
        logger.info(f"{RNAI_NAME} already exists.")

    latest_crispr_file_url = get_download_url(CRISPR_NAME)
    if force or not CRISPR_FILE.exists():
        download_pbar(latest_crispr_file_url, str(CRISPR_FILE))
    else:
        logger.info(f"{CRISPR_NAME} ({DEPMAP_RELEASE}) already exists.")

    model_info_url = get_download_url(MODEL_INFO_NAME)
    if force or not MODEL_INFO_FILE.exists():
        download_pbar(model_info_url, str(MODEL_INFO_FILE))
    else:
        logger.info(f"{MODEL_INFO_NAME} ({DEPMAP_RELEASE}) already exists.")

    if force or not MITOCARTA_FILE.exists():
        download_pbar(MITOCARTA_URL, str(MITOCARTA_FILE))
    else:
        logger.info(f"{MITOCARTA_NAME} already exists.")
