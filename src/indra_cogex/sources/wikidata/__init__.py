"""
This module contains processors for Wikidata sources.

- WikiDataProcessor: Base class for Wikidata processors
- Journal Publisher Processor: Processor for journal and publisher data

Other data:
- issn nlm ID map: Get this file from scopus.com -> sources -> select
'Journals' as source type -> click the down arrow/chevron next to
'Export to Excel' and select 'Select all' -> click 'Export to Excel'. The
page has a really strict timeout so if nothing happens, open the developer
console and check for 429 errors. If you see them, you have to wait at least
5 minutes before trying again.

"""
# Fixme: scopus only allows 1000 journals to be exported at a time,
#  i.e. if the list of journals is longer than 1000, only the first 1000
#  journals will be exported. There is no paging mechanism to get the rest of
#  the journals beyond the first 1000.

import csv
import gzip
import logging
from collections import namedtuple
from textwrap import dedent
from typing import List, Mapping, Any, Iterable

import pandas as pd
import requests
import tqdm

from indra_cogex.representation import Relation, Node
from indra_cogex.sources import Processor
from indra_cogex.sources.pubmed import issn_nlm_map_path, \
    process_mesh_xml_to_csv


__all__ = ["JournalPublisherProcessor"]


logger = logging.getLogger(__name__)


JournalPublisherTuple = namedtuple(
    "JournalPublisherTuple", [
        # Journal info
        "journal_wd_id",
        "journal_name",
        "journal_issn_list",
        "journal_issn_l",
        "nlm_id",
        # Journal metrics
        "citescore",
        "category_rank",
        "percentile",
        "category",
        "citations_2019_22",
        "documents_2019_22",
        "percent_cited_2019_22",
        "snip",
        "sjr",
        # Publisher info
        "publisher_wd_id",
        "publisher_name",
        "publisher_isni",
    ]
)


class WikiDataProcessor(Processor):
    """Base class for Wikidata processors"""
    name = "wikidata"
    importable = False
    sparql_query = NotImplemented
    WIKIDATA_ENDPOINT = \
        "https://query.wikidata.org/bigdata/namespace/wdq/sparql"

    def __init_subclass__(cls, **kwargs):
        # Modify the module path to include the name of this processor
        cls.module = cls.module.module(cls.name)

        # Now update paths for the node and relation data
        cls.directory = cls.module.base
        # These are nodes directly in the neo4j encoding
        cls.nodes_path = cls.module.join(name="nodes.tsv.gz")
        # These are nodes in the original INDRA-oriented representation
        # needed for assembly
        cls.nodes_indra_path = cls.module.join(name="nodes.pkl")
        cls.edges_path = cls.module.join(name="edges.tsv.gz")

    def get_nodes(self) -> Iterable[Node]:
        raise NotImplementedError(
            f"get_nodes not implemented in {self.__class__.__name__}. "
            f"Implement in a subclass."
        )

    def get_relations(self) -> Iterable[Relation]:
        raise NotImplementedError(
            f"get_relations not implemented in {self.__class__.__name__}. "
            f"Implement in a subclass."
        )

    def run_sparql_query(self, sparql: str) -> List[Mapping[str, Any]]:
        logger.debug("running query: %s", sparql)
        res = requests.get(self.WIKIDATA_ENDPOINT,
                           params={"query": sparql, "format": "json"})
        res.raise_for_status()
        res_json = res.json()
        return res_json["results"]["bindings"]


class JournalPublisherProcessor(WikiDataProcessor):
    """Processor for the Journal Publisher relations"""
    name = "journal_publisher"
    importable = True
    journal_node_type = "Journal"
    publisher_node_type = "Publisher"
    node_types = [publisher_node_type, journal_node_type]
    sparql_query = dedent("""
    SELECT DISTINCT
        ?journal
        ?journalLabel
        (group_concat(?issn;separator=",") as ?issn_list)
        ?issn_l
        ?publisher
        ?publisherLabel
        ?isni
    WHERE  
    {
        ?journal wdt:P31 wd:Q5633421 ;
                 rdfs:label ?journalLabel ;
                 wdt:P123 ?publisher .
        FILTER ( LANG(?journalLabel) = "en" )
        OPTIONAL { ?journal wdt:P236 ?issn }
        OPTIONAL { ?journal wdt:P7363 ?issn_l }
        OPTIONAL { ?publisher wdt:P213 ?isni }
        SERVICE wikibase:label {
            bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en".
        } # Helps get the label in your language, if not, then en language
    }
    GROUP BY ?journal ?journalLabel ?issn_l ?publisher ?publisherLabel ?isni
    """)

    def __init__(self):
        self.publisher_data_path = self.module.join(name="publisher_data.tsv.gz")
        self.journal_data_path = self.module.join(name="journal_data.tsv.gz")
        self.pub_jour_relations_data_path = self.module.join(
            name="pub_jour_relations_data.tsv.gz")
        self.scopus_citescore_path = self.module.join(
            name="scopus_citescore.xlsx")
        self.issn_nlm_map = None
        self.citescore_df = None

    @staticmethod
    def _load_issn_nlm_map():
        # First ensure the pre-processing has been done
        process_mesh_xml_to_csv()
        with gzip.open(issn_nlm_map_path, 'rt') as fh:
            reader = csv.reader(fh, delimiter=',')
            logger.info("Loading ISSN NLM map")
            issn_nlm_map = {issn: nlm_id for issn, nlm_id in reader}

        if not any(issn_nlm_map.values()):
            raise ValueError("ISSN NLM map is empty. Please check the "
                             "issn_nlm_map.csv.gz file generated by the "
                             "PubmedProcessor.")
        return issn_nlm_map

    def _load_citescore_map(self):
        if self.citescore_df is not None:
            return

        # Columns are:
        #   Source title: Journal name [str] <- maps to wikidata journal name
        #   CiteScore: 2019-22 CiteScore [float]
        #   Highest percentile: One string with three lines
        #       1. Percentile 0-99.0 % [float]
        #       2. Rank in Category (e.g. "2/80") [str]
        #       3. Category (e.g. "General Medicine")
        #   2019-22 Citations: Number of citations in 2019-22 [int]
        #   2019-22 Documents: ? [int]
        #   % Cited: ? [float]
        #   SNIP: ? [float]
        #   SJR: ? [float]
        #   Publisher: Publisher name [str] <- maps to wikidata publisher name

        # Load file (there is only one sheet)
        cs_df = pd.read_excel(
            self.scopus_citescore_path, engine="openpyxl", sheet_name=0
        ).dropna(subset=["Source title"])
        # Set column types (if other than string)
        cs_df["CiteScore"] = cs_df["CiteScore"].astype(float)
        cs_df["2019-22 Citations"] = cs_df["2019-22 Citations"].astype(int)
        cs_df["2019-22 Documents"] = cs_df["2019-22 Documents"].astype(int)
        cs_df["% Cited"] = cs_df["% Cited"].astype(float)
        cs_df["SNIP"] = cs_df["SNIP"].astype(float)
        cs_df["SJR"] = cs_df["SJR"].astype(float)

        # Split the percentile column into three columns
        cs_df[["Percentile", "Rank", "Category"]] = (
            cs_df["Highest percentile"].str.split("\n", expand=True)
        )
        cs_df["Percentile"] = \
            cs_df["Percentile"].str.replace("%", "").astype(float)
        # For rank, just keep the rank number (e.g. 2/80 -> 2) to allow sorting
        cs_df["Rank"] = cs_df["Rank"].str.split("/").str[0].astype(int)

        # Drop the original column
        cs_df = cs_df.drop(columns=["Highest percentile"])

        # Drop missing journal names; make the journal name the index
        cs_df = cs_df.dropna(subset=["Source title"])
        cs_df = cs_df.set_index("Source title")

        self.citescore_df = cs_df

    def iter_data(self):
        """Load data from Wikidata"""
        self.issn_nlm_map = self._load_issn_nlm_map()
        self._load_citescore_map()

        records = self.run_sparql_query(self.sparql_query)
        missing_citescore_data = 0
        for record in tqdm.tqdm(records, desc="Processing publisher wikidata"):
            # Wikidata
            journal_wd_id = record["journal"]["value"][
                            len("http://www.wikidata.org/entity/"):]
            journal_name = record["journalLabel"]["value"]
            journal_issn_list = record.get("issn_list", {}).get("value")
            if journal_issn_list:
                journal_issn_list = journal_issn_list.split(",")
            else:
                journal_issn_list = []
            journal_issn_l = record.get("issn_l", {}).get("value")
            publisher_isni = record.get("isni", {}).get("value")
            publisher_wd_id = record["publisher"]["value"][
                              len("http://www.wikidata.org/entity/"):
                              ]
            publisher_name = record["publisherLabel"]["value"]

            nlm_id = None
            for issn in [journal_issn_l] + journal_issn_list:
                if issn in self.issn_nlm_map:
                    nlm_id = self.issn_nlm_map[issn]
                    break

            # Skip if we don't have an NLM ID or an ISNI ID for the relation
            if nlm_id and publisher_isni:
                # CiteScore
                try:
                    citescore_row = self.citescore_df.loc[journal_name]
                except KeyError:
                    # Set all values to None
                    citescore_row = {
                        col: None for col in self.citescore_df.columns
                    }
                    missing_citescore_data += 1

                yield JournalPublisherTuple(
                    journal_wd_id=journal_wd_id,
                    journal_name=journal_name,
                    journal_issn_list=journal_issn_list,
                    journal_issn_l=journal_issn_l,
                    nlm_id=nlm_id,
                    citescore=citescore_row["CiteScore"],
                    category_rank=citescore_row["Rank"],
                    percentile=citescore_row["Percentile"],
                    category=citescore_row["Category"],
                    citations_2019_22=citescore_row["2019-22 Citations"],
                    documents_2019_22=citescore_row["2019-22 Documents"],
                    percent_cited_2019_22=citescore_row["% Cited"],
                    snip=citescore_row["SNIP"],
                    sjr=citescore_row["SJR"],
                    publisher_wd_id=publisher_wd_id,
                    publisher_name=publisher_name,
                    publisher_isni=publisher_isni
                )
        logger.info(
            f"Missing CiteScore data for {missing_citescore_data} journals"
        )

    def process_data(self, force: bool = False):
        """Dump data to CSV

        Parameters
        ----------
        force: bool
            If True, force the dump even if the files already exist
        """
        if self.publisher_data_path.exists() and \
                self.journal_data_path.exists() and \
                self.pub_jour_relations_data_path.exists() and \
                not force:
            logger.info("Files already exist, skipping dump.")
            return

        with gzip.open(self.publisher_data_path, 'wt') as publisher_fh, \
                gzip.open(self.journal_data_path, 'wt') as journal_fh, \
                gzip.open(self.pub_jour_relations_data_path, 'wt') as \
                        relations_fh:
            publisher_writer = csv.writer(publisher_fh, delimiter='\t')
            journal_writer = csv.writer(journal_fh, delimiter='\t')
            relations_writer = csv.writer(relations_fh, delimiter='\t')

            used_isni = set()
            used_nlm = set()
            nlm_isni_relations = set()
            skipped = 0
            for journal_publisher in self.iter_data():
                # Skip if no nlm_id or publisher_isni
                nlm_id = journal_publisher.nlm_id
                isni = journal_publisher.publisher_isni

                if not isni or not nlm_id:
                    skipped += 1
                    continue

                # Save relations
                # One relations file for journal-publisher relations
                # One row per relation:
                # nlm_id, publisher_isni
                if (nlm_id, isni) not in nlm_isni_relations:
                    relations_writer.writerow((nlm_id, isni))
                    nlm_isni_relations.add((nlm_id, isni))

                # Save publishers
                # One row per Publisher:
                # publisher_wd_id, publisher_name, publisher_isni
                if isni not in used_isni:
                    publisher_writer.writerow([
                        journal_publisher.publisher_wd_id,
                        journal_publisher.publisher_name,
                        isni
                    ])
                    used_isni.add(isni)

                # Save journals
                # One row per Journal:
                # journal_wd_id, journal_name, issn_list, journal_issn_l,
                # nlm_id, citescore, category_rank, percentile, category,
                # citations_2019_22, documents_2019_22, percent_cited_2019_22,
                # snip, sjr
                if journal_publisher.nlm_id not in used_nlm:
                    journal_writer.writerow([
                        journal_publisher.journal_wd_id,
                        journal_publisher.journal_name,
                        ";".join(journal_publisher.journal_issn_list or []),
                        journal_publisher.journal_issn_l,
                        journal_publisher.nlm_id,
                        journal_publisher.citescore,
                        journal_publisher.category_rank,
                        journal_publisher.percentile,
                        journal_publisher.category,
                        journal_publisher.citations_2019_22,
                        journal_publisher.documents_2019_22,
                        journal_publisher.percent_cited_2019_22,
                        journal_publisher.snip,
                        journal_publisher.sjr
                    ])
                    used_nlm.add(journal_publisher.nlm_id)

        logger.info(f"Dumped {len(used_isni)} publishers to "
                    f"{self.publisher_data_path}")
        logger.info(f"Dumped {len(used_nlm)} journals to "
                    f"{self.journal_data_path}")
        logger.info(f"Dumped {len(nlm_isni_relations)} relations to "
                    f"{self.pub_jour_relations_data_path}")
        logger.info(f"Skipped {skipped} relations due to missing NLM ID or "
                    f"ISNI")

    def get_nodes(self) -> Iterable[Node]:
        """Get nodes from the data"""
        self.process_data()

        # Get journal and publisher nodes
        yield from self._get_journal_nodes()
        yield from self._get_publisher_nodes()

    def _get_journal_nodes(self) -> Iterable[Node]:
        with gzip.open(self.journal_data_path, 'rt') as fh:
            reader = csv.reader(fh, delimiter='\t')
            # journal_wd_id, journal_name, issn_list, journal_issn_l, nlm_id,
            # citescore, category_rank, percentile, category,
            # citations_2019_22, documents_2019_22, percent_cited_2019_22,
            # snip, sjr
            for (
                    journal_wd_id,
                    journal_name,
                    issn_list_str,
                    journal_issn_l,
                    nlm_id,
                    citescore,
                    category_rank,
                    percentile,
                    category,
                    citations_2019_22,
                    documents_2019_22,
                    percent_cited_2019_22,
                    snip,
                    sjr
            ) in reader:
                yield Node(
                    "NLM",
                    nlm_id,
                    labels=[self.journal_node_type],
                    data={
                        "name": _get_val(journal_name),
                        "issn_l": _get_val(journal_issn_l),
                        "issn_list:string[]": _get_val(issn_list_str),
                        "wikidata_id": _get_val(journal_wd_id),
                        "citescore:float": _get_val(citescore),
                        "category_rank:int": _get_val(category_rank),
                        "percentile:float": _get_val(percentile),
                        "category": _get_val(category),
                        "citations_2019_22:int": _get_val(citations_2019_22),
                        "documents_2019_22:int": _get_val(documents_2019_22),
                        "percent_cited_2019_22:float": _get_val(
                            percent_cited_2019_22),
                        "snip:float": _get_val(snip),
                        "sjr:float": _get_val(sjr),
                    },
                )

    def _get_publisher_nodes(self) -> Iterable[Node]:
        with gzip.open(self.publisher_data_path, 'rt') as fh:
            reader = csv.reader(fh, delimiter='\t')
            for row in reader:
                publisher_wd_id, publisher_name, publisher_isni = row
                yield Node(
                    "ISNI",
                    # Strip the whitespace inside the ISNI
                    publisher_isni.replace(" ", ""),
                    labels=[self.publisher_node_type],
                    data={
                        "name": _get_val(publisher_name),
                        "wikidata_id": _get_val(publisher_wd_id),
                    },
                )

    def get_relations(self) -> Iterable[Relation]:
        """Get relations from the data"""
        self.process_data()

        # Get journal-publisher relations
        with gzip.open(self.pub_jour_relations_data_path, 'rt') as fh:
            reader = csv.reader(fh, delimiter='\t')
            for nlm_id, publisher_isni in reader:
                yield Relation(
                    source_ns="NLM",
                    source_id=nlm_id,
                    target_ns="ISNI",
                    # Strip the whitespace inside the ISNI
                    target_id=publisher_isni.replace(" ", ""),
                    rel_type="published_by",
                    data={},
                )


def _get_val(val):
    if (
        pd.isna(val) or
        isinstance(val, str) and not val.strip() or
        isinstance(val, str) and val == "nan"
    ):
        return None
    else:
        return val
