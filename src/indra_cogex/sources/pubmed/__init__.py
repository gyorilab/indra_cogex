import csv
import gzip
import json
import logging
import os
import re
from hashlib import md5
from itertools import chain
from typing import Tuple, Mapping, Iterable, List, Set

import textwrap
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from lxml import etree
from tqdm.std import tqdm
from indra.util import batch_iter
from indra.literature import pubmed_client
from indra_cogex.sources.utils import get_bool
from indra_cogex.representation import Node, Relation
from indra_cogex.sources.processor import Processor
from indra_cogex.sources.pubmed.locations import *
from indra_cogex.sources.indra_db.locations import text_refs_fname

logger = logging.getLogger(__name__)

pubmed_base_url = "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/"
pubmed_update_url = "https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/"


class PubmedProcessor(Processor):
    name = "pubmed"
    publication_node_type = "Publication"
    journal_node_type = "Journal"
    node_types = [publication_node_type, journal_node_type]

    def __init__(self):
        # Maps MeSH terms to PMIDs
        self.mesh_pmid_path = mesh_pmid_path
        # Maps PMIDs to years and publication types
        self.pmid_year_types_path = pmid_year_types_path
        # Maps PMIDs to ISSN
        self.pmid_nlm_path = pmid_nlm_path
        # Identifies journals
        self.journal_info_path = journal_info_path
        # Maps PMIDs to other text reference IDs
        self.text_refs_path = text_refs_fname

    def get_nodes(self) -> Iterable[Node]:
        process_mesh_xml_to_csv(
            mesh_pmid_fpath=self.mesh_pmid_path,
            pmid_year_types_fpath=self.pmid_year_types_path,
            pmid_nlm_fpath=self.pmid_nlm_path,
            journal_info_fpath=self.journal_info_path,
        )
        yield from self._yield_publication_nodes()
        yield from self._yield_journal_nodes()

    def _yield_publication_nodes(self) -> Iterable[Node]:
        logger.info("Loading PMID year info from %s" % self.pmid_year_types_path)
        with gzip.open(self.pmid_year_types_path, "rt") as fh:
            pmid_years_pubtypes = {
                pmid: (year, json.loads(types))
                for pmid, year, types in csv.reader(fh, delimiter="\t")
            }
        logger.info("Loaded PMID year info from %s" % self.pmid_year_types_path)

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
                year, pubtypes = pmid_years_pubtypes.get(pmid, (None, []))
                data = {
                    "trid": get_val(trid),
                    "pmcid": get_val(pmcid),
                    "doi": get_val(doi),
                    "pii": get_val(pii),
                    "url": get_val(url),
                    "manuscript_id": get_val(manuscript_id),
                    "year:int": year,
                    "publication_type:string[]": ";".join(pubtypes),
                    "retracted:boolean": get_bool("Retracted Publication" in pubtypes)
                }
                yield Node(
                    "PUBMED",
                    pmid,
                    labels=[self.publication_node_type],
                    data=data,
                )

    def _yield_journal_nodes(self) -> Iterable[Node]:
        # Load the journal info
        logger.info("Loading journal info from %s" % self.journal_info_path)
        with gzip.open(self.journal_info_path, "rt") as fh:
            reader = csv.reader(fh, delimiter="\t")
            next(reader)  # skip header
            for (
                    nlm_id,
                    journal_name,
                    journal_abbrev,
                    issn,
                    issn_l,
                    p_issn,
                    e_issn,
                    other
            ) in reader:
                other = json.loads(other) or []
                assert isinstance(other, list)
                data = {
                    "title": journal_name,
                    "abbr_title": journal_abbrev,
                    "issn_l": issn_l,
                    "p_issn": p_issn,
                    "e_issn": e_issn,
                    "alternate_issn:string[]": ";".join(other),
                }
                yield Node(
                    "NLM",
                    nlm_id,
                    labels=[self.journal_node_type],
                    data=data,
                )

    def get_relations(self) -> Iterable[List[Relation]]:
        # Ensure cached files exist
        # Todo: Add force option to download files?
        process_mesh_xml_to_csv(
            mesh_pmid_fpath=self.mesh_pmid_path,
            pmid_year_types_fpath=self.pmid_year_types_path,
            pmid_nlm_fpath=self.pmid_nlm_path,
            journal_info_fpath=self.journal_info_path,
        )
        logger.info("Generating mesh-pmid relations")
        yield from self._yield_mesh_pmid_relations()
        logger.info("Generating pmid-journal relations")
        yield from self._yield_pmid_journal_relations()

    def _yield_mesh_pmid_relations(self) -> Iterable[List[Relation]]:
        with gzip.open(self.mesh_pmid_path, "rt") as fh:
            reader = csv.reader(fh)
            next(reader)  # skip header
            # NOTE tested with 100000 batch size but given that total is ~290M
            # and each line is lightweight, trying with larger batch here
            batch_size = 10_000_000
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
                            data={
                                "is_major_topic:boolean": get_bool(major_topic == "1")
                            },
                        )
                    )
                yield relations_batch

    def _yield_pmid_journal_relations(self) -> Iterable[List[Relation]]:
        # Yield batches of relations
        with gzip.open(self.pmid_nlm_path, "rt") as fh:
            reader = csv.reader(fh)
            # Skip header
            next(reader)
            # The file has more than 35000000 lines - a batch size of 1M is
            # reasonable
            batch_size = 1_000_000
            for batch in batch_iter(
                    reader, batch_size=batch_size, return_func=list
            ):
                relations_batch = []
                for pmid, journal_nlm_id in batch:
                    relations_batch.append(
                        Relation(
                            "PUBMED",
                            pmid,
                            "NLM",
                            journal_nlm_id,
                            "published_in",
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


def get_url_paths(url: str) -> Iterable[str]:
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
) -> Iterable[Tuple[str, int, Mapping, Mapping, Set[str]]]:
    """Extract info from medline xml file.

    Parameters
    ----------
    xml_path :
        Path to medline xml.gz file.

    Yields
    ------
    :
        Tuple of (
            PMID,
            year,
            MeSH annotations,
            journal info,
            publication type tags
        )
    """
    tree = etree.parse(xml_path)

    for article in tree.findall("PubmedArticle"):
        medline_citation = article.find("MedlineCitation")
        years = list(
            medline_citation.findall("Article/Journal/JournalIssue/PubDate/Year")
        ) + list(article.findall("PubmedData/History/PubMedPubDate/Year"))
        min_year = min((int(year.text) for year in years), default=None)
        pmid = medline_citation.find("PMID").text
        if min_year is None:
            logger.warning(f"Could not find year for PMID {pmid}")

        mesh_annotations = pubmed_client._get_annotations(medline_citation)
        journal_info = pubmed_client.get_issn_info(
            medline_citation, get_issns_from_nlm="missing"
        )
        pub_tags = pubmed_client.get_publication_types(article)
        yield (
            pmid,
            min_year,
            mesh_annotations["mesh_annotations"],
            journal_info,
            pub_tags
        )


def process_mesh_xml_to_csv(
    mesh_pmid_fpath: Path = mesh_pmid_path,
    pmid_year_types_fpath: Path = pmid_year_types_path,
    pmid_nlm_fpath: Path = pmid_nlm_path,
    journal_info_fpath: Path = journal_info_path,  # For Journal Node creation
    force: bool = False
):
    """Process the pubmed xml and dump to different CSV files

    Dump to CSV file with the columns: mesh_id,is_concept,major_topic,pmid

    Parameters
    ----------
    mesh_pmid_fpath :
        Path to the mesh pmid file
    pmid_year_types_fpath :
        Path to the pmid, year, publication types file
    pmid_nlm_fpath :
        Path to the pmid journal file
    journal_info_fpath :
        Path to the journal info file, used to create the Journal Nodes
    force :
        If True, re-run the download even if the file already exists.
    """
    # Todo: Some of the pipeline could be replaced with
    #  raw_xml.ensure(url=xml_gz_url) though this makes the md5 check
    #  cumbersome.

    if not force and mesh_pmid_fpath.exists() and pmid_year_types_fpath.exists() and \
            pmid_nlm_fpath.exists() and journal_info_fpath.exists():
        logger.info(
            f"{mesh_pmid_fpath.name}, {pmid_year_types_fpath.name}, "
            f"{pmid_nlm_fpath.name} and {journal_info_fpath.name} "
            f"already exist, skipping download"
        )
        return

    # Check resource files and download missing ones first
    download_medline_pubmed_xml_resource(force=force)

    # Loop the stowed xml files
    logger.info("Processing PubMed XML files")
    with gzip.open(mesh_pmid_fpath, "wt") as fh_mesh, \
            gzip.open(pmid_year_types_fpath, "wt") as fh_year_types, \
            gzip.open(pmid_nlm_fpath, "wt") as fh_journal, \
            gzip.open(journal_info_fpath, "wt") as fh_journal_info, \
            gzip.open(issn_nlm_map_path, "wt") as fh_issn_nlm_map:

        # Get the CSV writers
        writer_mesh = csv.writer(fh_mesh, delimiter=",")
        writer_year_types = csv.writer(fh_year_types, delimiter="\t")
        writer_journal = csv.writer(fh_journal, delimiter=",")
        writer_journal_info = csv.writer(fh_journal_info, delimiter="\t")
        writer_issn_nlm_map = csv.writer(fh_issn_nlm_map, delimiter=",")

        # Write the headers
        writer_mesh.writerow(["mesh_id", "major_topic", "pmid"])
        # Why no file header for the year file?
        writer_journal.writerow(
            ["pmid", "journal_nlm_id"]
        )
        writer_journal_info.writerow(
            ["journal_nlm_id", "journal_name", "journal_abbrev",
             "issn", "issn_l", "p_issn", "e_issn", "other"]
        )
        writer_issn_nlm_map.writerow(["issn", "nlm_id"])
        used_nlm_ids = set()
        yielded_pmid_nlm_links = set()
        yielded_issn_nlm_links = set()
        for _, xml_path, _ in xml_path_generator(description="XML to CSV"):
            for (
                    pmid, year, mesh_annotations, journal_info, publication_types
            ) in extract_info_from_medline_xml(xml_path.as_posix()):
                # Skip if year could not be found
                if not year:
                    continue

                # Write one row per mesh annotation
                for annot in mesh_annotations:
                    writer_mesh.writerow(
                        [
                            annot["mesh"],
                            1 if annot["major_topic"] else 0,
                            pmid
                        ]
                    )

                # One row per pmid,year,publication type
                writer_year_types.writerow(
                    [pmid, year, json.dumps(sorted(publication_types))]
                )

                # One row per nlm_id-pmid connection
                # issn_dict structure:
                # {
                #    "issn": "1234-5678",
                #    "issn_l": "1234-5678",
                #    "issn_type": "electronic"|"print"|"other",
                #    "alternate_issns": [
                #        ("linking"|"electronic"|"print"|"other", "1234-5678"),
                #        ...
                #    ],
                issn_dict = journal_info["issn_dict"]
                nlm_id = journal_info["journal_nlm_id"]
                pmid_nlm_link = (pmid, nlm_id)
                if pmid_nlm_link not in yielded_pmid_nlm_links:
                    writer_journal.writerow(pmid_nlm_link)
                    yielded_pmid_nlm_links.add(pmid_nlm_link)

                # Get all issns
                issn_set = {issn_dict.get("issn"), issn_dict.get("issn_l")}
                if issn_dict.get("alternate_issns"):
                    issn_set |= {
                        issn for _, issn in issn_dict["alternate_issns"]
                    }

                # Remove None
                issn_set -= {None}

                # One row per issn-nlm_id connection
                for issn in issn_set:
                    issn_nlm_link = (issn, nlm_id)
                    if issn_nlm_link not in yielded_issn_nlm_links:
                        writer_issn_nlm_map.writerow(issn_nlm_link)
                        yielded_issn_nlm_links.add(issn_nlm_link)

                # One row per journal, i.e. nlm id
                if nlm_id not in used_nlm_ids:
                    issn_type = issn_dict.get("issn_type", "other")
                    issn = issn_dict.get("issn")
                    issn_l = issn_dict.get("issn_l")
                    if issn_type == "electronic":
                        e_issn = issn
                        p_issn = None
                    elif issn_type == "print":
                        e_issn = None
                        p_issn = issn
                    else:
                        e_issn = None
                        p_issn = None

                    if issn_dict.get("alternate_issns"):
                        other_issns = set()
                        for issn_type, issn_val in issn_dict["alternate_issns"]:
                            if issn_type == "electronic" and not e_issn:
                                e_issn = issn_val
                            elif issn_type == "print" and not p_issn:
                                p_issn = issn_val
                            elif issn_type not in ("electronic", "print"):
                                other_issns.add(issn_val)
                        other_issns -= {issn, issn_l, None}
                    else:
                        other_issns = set()
                    writer_journal_info.writerow(
                        [
                            nlm_id,
                            journal_info["journal_title"],
                            journal_info["journal_abbrev"],
                            issn,
                            issn_l,
                            p_issn,
                            e_issn,
                            json.dumps(list(other_issns))
                        ]
                    )
                    used_nlm_ids.add(journal_info["journal_nlm_id"])


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
        If True, will raise error instead of skipping the file when
        downloading. Default: False.
    """
    for xml_file, stow, base_url in xml_path_generator(description="Download"):
        # Check if resource already exists
        if not force and stow.exists():
            continue

        # Download the resource
        response = requests.get(base_url + xml_file)
        if response.status_code != 200:
            if raise_http_error:
                response.raise_for_status()
            else:
                logger.warning(
                    f"Skipping {xml_file} due to HTTP status {response.status_code}"
                )
                continue

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
) -> Iterable[Tuple[str, Path, str]]:
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
