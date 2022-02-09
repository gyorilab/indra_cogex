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
                for mesh_num, is_concept, major_topic, pmid in batch:
                    mesh_id = mesh_num_to_id(mesh_num, int(is_concept))
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


def download_medline_pubmed_xml_resource(force: bool = False) -> None:
    """Downloads the medline and pubmed data from the NCBI ftp site.

    The location of the downloaded data is determined by pystow

    Parameters
    ----------
    force :
        If True, will download a file even if it already exists.
    """
    for i in tqdm(
        range(1, max_update_index + 1),
        total=max_file_index,
        desc="Download medline pubmed xml resource files",
    ):
        # Assemble the file name and the resource path
        xml_file = xml_file_temp.format(index=str(i).zfill(4))
        stow = raw_xml.join(name=xml_file)

        # Check if resource already exists
        if not force and stow.exists():
            logger.info(f"{stow} already exists, skipping download.")
            continue

        # Download the resource; if index <= max_update_index, then
        # the resource file is from the /baseline directory, otherwise it's
        # from the /updatefiles directory on the server
        url = pubmed_base_url if i <= max_file_index else pubmed_update_url
        response = requests.get(url + xml_file + ".gz")
        md5_response = requests.get(url + xml_file + ".gz.md5")
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
