import csv
import gzip
import json
import logging
import os
import re
from hashlib import md5
from typing import Tuple, Generator

import pystow
import textwrap
from pathlib import Path

import requests
from lxml import etree
from tqdm.std import tqdm
from indra.util import batch_iter
from indra_cogex.representation import Node, Relation
from indra_cogex.sources.processor import Processor


logger = logging.getLogger(__name__)

resources = pystow.module("indra", "cogex", "pubmed")

MESH_PMID = resources.join(name="mesh_pmids.csv")
PMID_YEAR = resources.join(name="pmid_years_07-2021.json")
TEXT_REFS = resources.join(name="text_refs.tsv.gz")

# Settings for downloading content from the PubMed FTP server
raw_xml = pystow.module("indra", "cogex", "pubmed", "raw_xml")
year_index = 22
max_file_index = 1114
max_update_index = 1186
xml_file_temp = "pubmed%sn{index}.xml" % year_index
pubmed_base_url = "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/"
pubmed_update_url = "https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/"


class PubmedProcessor(Processor):
    name = "pubmed"
    node_types = ["Publication"]

    def __init__(
        self,
        mesh_pmid_path=MESH_PMID,
        pmid_year_path=PMID_YEAR,
    ):
        self.mesh_pmid_path = mesh_pmid_path
        self.pmid_year_path = pmid_year_path
        self.text_refs_path = pystow.join("indra", "db", name="text_refs.tsv.gz")
        # Check if the files exist without loading them
        for path in [mesh_pmid_path, pmid_year_path]:
            if not path.exists():
                raise FileNotFoundError(f"No such file: {path}")

    def get_nodes(self):
        pmid_node_type = "Publication"
        logger.info("Loading PMID year info from %s" % self.pmid_year_path)
        with open(self.pmid_year_path, "r") as fh:
            pmid_years = json.load(fh)
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
        process_mesh_xml_to_csv(mesh_pmid_path=self.mesh_pmid_path)

        with open(self.mesh_pmid_path, "r") as fh:
            reader = csv.reader(fh)
            next(reader)  # skip header
            # NOTE tested with 100000 batch size but given that total is ~290M
            # and each line is lightweight, trying with larger batch here
            batch_size = 1000000
            for batch in tqdm(
                batch_iter(reader, batch_size=batch_size, return_func=list)
            ):
                relations_batch = []
                for mesh_id, is_concept, major_topic, pmid in batch:
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


def mesh_num_to_id(mesh_num, is_concept):
    prefix = "C" if is_concept else "D"
    if prefix == "D":
        if int(mesh_num) < 66332:
            mesh_num = str(mesh_num).zfill(6)
        else:
            mesh_num = str(mesh_num).zfill(9)
    elif prefix == "C":
        if int(mesh_num) < 588418:
            mesh_num = str(mesh_num).zfill(6)
        else:
            mesh_num = str(mesh_num).zfill(9)
    return prefix + mesh_num


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
) -> Generator[Tuple[str, int, bool, int], None, None]:
    """Extract info from medline xml file.

    Parameters
    ----------
    xml_path :
        Path to medline xml file.

    Yields
    ------
    :
        Tuple of (pmid, year, is_concept, mesh_num).
    """
    tree = etree.parse(xml_path)
    elements = tree.xpath("//MedlineCitation")
    for element in elements:
        pmid_element = element.xpath("PMID")[0]
        pmid = int(pmid_element.text)
        mesh_heading_list = element.xpath("MeshHeadingList")
        if not mesh_heading_list:
            continue
        mesh_heading_list = mesh_heading_list[0]
        for mesh_element in mesh_heading_list.getchildren():
            descriptor = mesh_element.xpath("DescriptorName")[0]
            attributes = descriptor.attrib
            mesh_id = attributes["UI"]
            is_concept = 1 if mesh_id[0] == "C" else 0
            major_topic = attributes["MajorTopicYN"] == "Y"
            yield mesh_id, is_concept, major_topic, pmid


def process_mesh_xml_to_csv(mesh_pmid_path: Path = MESH_PMID, force: bool = False):
    """Process the pubmed xml and dump to a CSV file

    Dump to CSV file with the columns: mesh_id,is_concept,major_topic,pmid

    Parameters
    ----------
    mesh_pmid_path :
        Path to the mesh pmid file
    force :
        If True, re-run the download even if the file already exists.
    """
    # Run the check and download first
    download_medline_pubmed_xml_resource(force=force)

    # Loop the stowed xml files
    logger.info("Processing xml files to CSV")
    with mesh_pmid_path.open("w") as fh:
        writer = csv.writer(fh, delimiter=",")
        writer.writerow(["mesh_id", "is_concept", "major_topic", "pmid"])
        for _, xml_path, _ in xml_path_generator(description="XML to CSV"):
            writer.writerows(extract_info_from_medline_xml(xml_path.as_posix()))


def download_medline_pubmed_xml_resource(force: bool = False) -> None:
    """Downloads the medline and pubmed data from the NCBI ftp site.

    The location of the downloaded data is determined by pystow

    Parameters
    ----------
    force :
        If True, will download a file even if it already exists.
    """
    for xml_file, stow, base_url in xml_path_generator(description="Download"):
        # Check if resource already exists
        if not force and stow.exists():
            continue

        # Download the resource
        response = requests.get(base_url + xml_file + ".gz")
        md5_response = requests.get(base_url + xml_file + ".gz.md5")
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
        with stow.open("w") as f:
            f.write(gzip.decompress(response.content).decode("utf-8"))


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

    def _get_tuple(ix: int) -> Tuple[str, Path, str]:
        file = xml_file_temp.format(index=str(ix).zfill(4))
        stow = raw_xml.join(name=file)

        # If index <= max_update_index, then the resource file is from the
        # /baseline directory, otherwise it's from the /updatefiles
        # directory on the server
        base_url = pubmed_base_url if ix <= max_file_index else pubmed_update_url
        return file, stow, base_url

    if bar:
        for i in tqdm(
            range(1, max_update_index + 1),
            total=max_update_index + 1,
            desc=description,
        ):
            yield _get_tuple(i)
    else:
        for i in range(1, max_update_index + 1):
            yield _get_tuple(i)
