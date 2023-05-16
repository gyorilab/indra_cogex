import csv
import gzip
import logging
import os
import re
from hashlib import md5
from itertools import chain
from typing import Tuple, Generator, Mapping

import pystow
import textwrap
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from lxml import etree
from tqdm.std import tqdm
from indra.util import batch_iter
from indra.literature.pubmed_client import _get_annotations
from indra_cogex.representation import Node, Relation
from indra_cogex.sources.processor import Processor


logger = logging.getLogger(__name__)

resources = pystow.module("indra", "cogex", "pubmed")

# Settings for downloading content from the PubMed FTP server
raw_xml = pystow.module("indra", "cogex", "pubmed", "raw_xml")
pubmed_base_url = "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/"
pubmed_update_url = "https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/"


class PubmedProcessor(Processor):
    name = "pubmed"
    node_types = ["Publication"]

    def __init__(self):
        self.mesh_pmid_path = resources.join(name="mesh_pmids.csv.gz")
        self.pmid_year_path = resources.join(name="pmid_years.csv.gz")
        self.text_refs_path = pystow.join("indra", "db", name="text_refs_principal.tsv.gz")

    def get_nodes(self):
        pmid_node_type = "Publication"
        process_mesh_xml_to_csv(
            mesh_pmid_path=self.mesh_pmid_path, pmid_year_path=self.pmid_year_path
        )
        logger.info("Loading PMID year info from %s" % self.pmid_year_path)
        with gzip.open(self.pmid_year_path, "rt") as fh:
            pmid_years = {pmid: year for pmid, year in csv.reader(fh)}
        logger.info("Loaded PMID year info from %s" % self.pmid_year_path)

        def get_val(val):
            # postgres exports \N for missing values
            if val == "\\N":
                return None
            else:
                return val

        # We iterate over text refs to get the nodes and
        # then look up the year to add as a property
        ensure_text_refs(self.text_refs_path.as_posix())
        with gzip.open(self.text_refs_path, "rt", encoding="utf-8") as fh:
            reader = csv.reader(fh, delimiter="\t")
            for trid, pmid, pmcid, doi, pii, url, manuscript_id in reader:
                if not get_val(pmid):
                    continue
                year = pmid_years.get(pmid, None)
                data = {
                    "trid": get_val(trid),
                    "pmcid": get_val(pmcid),
                    "doi": get_val(doi),
                    "pii": get_val(pii),
                    "url": get_val(url),
                    "manuscript_id": get_val(manuscript_id),
                    "year:int": year,
                }
                yield Node("PUBMED", pmid, labels=[pmid_node_type], data=data)

    def get_relations(self):
        # Ensure cached files exist
        # Todo: Add force option to download files?
        process_mesh_xml_to_csv(
            mesh_pmid_path=self.mesh_pmid_path, pmid_year_path=self.pmid_year_path
        )

        with gzip.open(self.mesh_pmid_path, "rt") as fh:
            reader = csv.reader(fh)
            next(reader)  # skip header
            # NOTE tested with 100000 batch size but given that total is ~290M
            # and each line is lightweight, trying with larger batch here
            batch_size = 10000000
            for batch in tqdm(
                batch_iter(reader, batch_size=batch_size, return_func=list)
            ):
                relations_batch = []
                for mesh_id, major_topic, pmid in batch:
                    relations_batch.append(
                        Relation(
                            "PUBMED",
                            pmid,
                            "MESH",
                            mesh_id,
                            "annotated_with",
                            {
                                "is_major_topic:boolean": "true"
                                if major_topic == "1"
                                else "false"
                            },
                        )
                    )
                yield relations_batch

    def _dump_edges(self) -> Path:
        # This overrides the default implementation in Processor because
        # we want to process relations in batches
        for bidx, batch in enumerate(self.get_relations()):
            logger.info(f"Dumping relations batch {bidx}")
            sample_path = None
            write_mode = "at"
            if bidx == 0:
                sample_path = self.module.join(name="edges_sample.tsv")
                write_mode = "wt"
            edges_path = self._dump_edges_to_path(
                batch, self.edges_path, sample_path, write_mode
            )
        return edges_path


def get_url_paths(url: str) -> Generator:
    """Get the paths to all XML files on the PubMed FTP server."""
    logger.info("Getting URL paths from %s" % url)

    # Get page
    response = requests.get(url)
    response.raise_for_status()

    # Make soup
    soup = BeautifulSoup(response.text, "html.parser")

    # Append trailing slash if not present
    url = url if url.endswith("/") else url + "/"

    # Loop over all links
    for link in soup.find_all("a"):
        href = link.get("href")
        # yield if href matches
        # 'pubmed<2 digit year>n<4 digit file index>.xml.gz'
        # but skip the md5 files
        if href and href.startswith("pubmed") and href.endswith(".xml.gz"):
            yield url + href


def ensure_text_refs(fname):
    if os.path.exists(fname):
        logger.info(f"Found existing text refs in {fname}")
        return
    from indra_db import get_db

    db = get_db("primary")
    os.environ["PGPASSWORD"] = db.url.password
    logger.info(f"Dumping text refs into {fname}")
    command = textwrap.dedent(
        f"""
        psql -d {db.url.database} -h {db.url.host} -U {db.url.username}
        -c "COPY (SELECT id, pmid, pmcid, doi, pii, url, manuscript_id 
        FROM public.text_ref) TO STDOUT"
        | gzip > {fname}
    """
    ).replace("\n", " ")
    os.system(command)


def extract_info_from_medline_xml(
    xml_path: str,
) -> Generator[Tuple[str, int, Mapping], None, None]:
    """Extract info from medline xml file.

    Parameters
    ----------
    xml_path :
        Path to medline xml.gz file.

    Yields
    ------
    :
        Tuple of (PMID, year, MeSH annotations).
    """
    tree = etree.parse(xml_path)

    for article in tree.findall("PubmedArticle"):
        medline_citation = article.find("MedlineCitation")
        years = list(
            medline_citation.findall("Article/Journal/JournalIssue/PubDate/Year")
        ) + list(article.findall("PubmedData/History/PubMedPubDate/Year"))
        min_year = min(int(year.text) for year in years)
        pmid = medline_citation.find("PMID").text

        mesh_annotations = _get_annotations(medline_citation)
        yield pmid, min_year, mesh_annotations["mesh_annotations"]


def process_mesh_xml_to_csv(mesh_pmid_path, pmid_year_path, force: bool = False):
    """Process the pubmed xml and dump to a CSV file

    Dump to CSV file with the columns: mesh_id,is_concept,major_topic,pmid

    Parameters
    ----------
    mesh_pmid_path :
        Path to the mesh pmid file
    force :
        If True, re-run the download even if the file already exists.
    """
    # Todo: Some of the pipeline could be replaced with
    #  raw_xml.ensure(url=xml_gz_url) though this makes the md5 check
    #  cumbersome.

    if not force and mesh_pmid_path.exists() and pmid_year_path.exists():
        logger.info(f"{mesh_pmid_path.name} and {pmid_year_path.name} already exist")
        return

    # Check resource files and download missing ones first
    download_medline_pubmed_xml_resource(force=force)

    # Loop the stowed xml files
    logger.info("Processing PubMed XML files")
    with gzip.open(mesh_pmid_path, "wt") as fh, gzip.open(
        pmid_year_path, "wt"
    ) as fh_year:
        writer = csv.writer(fh, delimiter=",")
        writer.writerow(["mesh_id", "major_topic", "pmid"])
        writer_year = csv.writer(fh_year, delimiter=",")
        for _, xml_path, _ in xml_path_generator(description="XML to CSV"):
            for pmid, year, mesh_annotations in extract_info_from_medline_xml(
                xml_path.as_posix()
            ):
                for annot in mesh_annotations:
                    writer.writerow(
                        (annot["mesh"], 1 if annot["major_topic"] else 0, pmid)
                    )
                writer_year.writerow([pmid, year])


def download_medline_pubmed_xml_resource(
    force: bool = False,
    raise_http_error: bool = False
) -> None:
    """Downloads the medline and pubmed data from the NCBI ftp site.

    The location of the downloaded data is determined by pystow

    Parameters
    ----------
    force :
        If True, will download a file even if it already exists.
    raise_http_error :
        If True, will ignore HTTP errors when downloading the files.
        Default: False.
    """
    for xml_file, stow, base_url in xml_path_generator(description="Download"):
        # Check if resource already exists
        if not force and stow.exists():
            continue

        # Download the resource
        response = requests.get(base_url + xml_file)
        if not raise_http_error:
            response.raise_for_status()
        md5_response = requests.get(base_url + xml_file + ".md5")
        actual_checksum = md5(response.content).hexdigest()
        expected_checksum = re.search(
            r"[0-9a-z]+(?=\n)", md5_response.content.decode("utf-8")
        ).group()
        if actual_checksum != expected_checksum:
            logger.warning(
                f"Checksum does not match for {xml_file}. Is index out of bounds?"
            )
            continue

        # PyStow the file
        with stow.open("wb") as f:
            f.write(response.content)


def xml_path_generator(
    bar: bool = True, description: str = "Looping xml paths"
) -> Generator[Tuple[str, Path, str], None, None]:
    """Returns a generator of (xml_file, xml_path, base_url) tuples.

    Parameters
    ----------
    bar :
        If True, will display a progress bar.
    description :
        Description of the progress bar.

    Yields
    ------
    :
        Tuple of (xml_file, xml_path, xml_url).
    """

    def _get_tuple(url: str) -> Tuple[str, Path, str]:
        file = url.split("/")[-1]
        stow = raw_xml.join(name=file)

        # Get the base url
        base_url = url.replace(file, "")
        return file, stow, base_url

    baseline_urls = get_url_paths(pubmed_base_url)
    update_urls = get_url_paths(pubmed_update_url)

    if bar:
        baseline_urls = [u for u in baseline_urls]
        update_urls = [u for u in update_urls]
        all_urls = baseline_urls + update_urls
        for pubmed_url in tqdm(
            all_urls,
            desc=description,
        ):
            yield _get_tuple(pubmed_url)
    else:
        for pubmed_url in chain(baseline_urls, update_urls):
            yield _get_tuple(pubmed_url)
