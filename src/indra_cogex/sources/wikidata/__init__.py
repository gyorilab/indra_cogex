import csv
import gzip
import logging
from collections import namedtuple
from textwrap import dedent
from typing import List, Mapping, Any, Iterable

import pystow
import requests
import tqdm

from indra_cogex.representation import Relation, Node
from indra_cogex.sources import Processor
from indra_cogex.sources.pubmed import issn_nlm_map_path


__all__ = ["JournalPublisherProcessor", "WikiDataProcessor"]


logger = logging.getLogger(__name__)


resources = pystow.module("indra_cogex", "sources", "wikidata")


JournalPublisherTuple = namedtuple(
    "JournalPublisherTuple", [
        "journal_wd_id",
        "journal_name",
        "journal_issn_list",
        "journal_issn_l",
        "nlm_id",
        "publisher_wd_id",
        "publisher_name",
        "publisher_isni",
    ]
)


class WikiDataProcessor(Processor):
    """Base class for Wikidata processors"""

    sparql_query = NotImplemented
    WIKIDATA_ENDPOINT = \
        "https://query.wikidata.org/bigdata/namespace/wdq/sparql"

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
        self.publisher_data_path = resources.join(name="publisher_data.tsv.gz")
        self.journal_data_path = resources.join(name="journal_data.tsv.gz")
        self.pub_jour_relations_data_path = resources.join(
            name="pub_jour_relations_data.tsv.gz")
        self.issn_nlm_map = self._load_issn_nlm_map()

    def _load_issn_nlm_map(self):
        with gzip.open(issn_nlm_map_path, 'rt') as fh:
            reader = csv.reader(fh, delimiter=',')
            issn_nlm_map = {issn: nlm_id for issn, nlm_id in reader}
        return issn_nlm_map

    def iter_data(self):
        """Load data from Wikidata"""
        records = self.run_sparql_query(self.sparql_query)
        for record in tqdm.tqdm(records, desc="Processing Publisher wikidata"):
            journal_wd_id = record["journal"]["value"][
                            len("http://www.wikidata.org/entity/"):]
            journal_name = record["journalLabel"]["value"]
            journal_issn_list = record.get("issn_list", {}).get("value")
            if journal_issn_list:
                journal_issn_list = journal_issn_list.split(",")
            else:
                journal_issn_list = []
            journal_issn_l = record.get("issn_l", {}).get("value")
            publisher_isni = record.get("value", {}).get("isni")
            publisher_wd_id = record["publisher"]["value"][
                              len("http://www.wikidata.org/entity/"):
                              ]
            publisher_name = record["publisherLabel"]["value"]
            nlm_id = self.issn_nlm_map.get(journal_issn_l)
            # Skip if we don't have an NLM ID or an ISNI ID for the relation
            if nlm_id and publisher_isni:
                yield JournalPublisherTuple(
                    journal_wd_id=journal_wd_id,
                    journal_name=journal_name,
                    journal_issn_list=journal_issn_list,
                    journal_issn_l=journal_issn_l,
                    nlm_id=nlm_id,
                    publisher_wd_id=publisher_wd_id,
                    publisher_name=publisher_name,
                    publisher_isni=publisher_isni
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
            for journal_publisher in self.iter_data():
                # Skip if no nlm_id or publisher_isni
                if not journal_publisher.publisher_isni or \
                        not journal_publisher.nlm_id:
                    continue
                nlm_id = journal_publisher.nlm_id
                isni = journal_publisher.publisher_isni

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
                # journal_wd_id, journal_name, issn_list, journal_issn_l, nlm_id
                if journal_publisher.nlm_id not in used_nlm:
                    journal_writer.writerow([
                        journal_publisher.journal_wd_id,
                        journal_publisher.journal_name,
                        journal_publisher.journal_issn_list,
                        journal_publisher.journal_issn_l,
                        journal_publisher.nlm_id,
                    ])

    def get_nodes(self) -> Iterable[Node]:
        """Get nodes from the data"""
        self.process_data()

        # Get journal and publisher nodes
        yield from self._get_journal_nodes()
        yield from self._get_publisher_nodes()

    def _get_journal_nodes(self) -> Iterable[Node]:
        with gzip.open(self.journal_data_path, 'rt') as fh:
            reader = csv.reader(fh, delimiter='\t')
            # journal_wd_id, journal_name, issn_list, journal_issn_l, nlm_id
            for (
                    journal_wd_id,
                    journal_name,
                    issn_list,
                    journal_issn_l,
                    nlm_id
            ) in reader:
                assert isinstance(issn_list, list)
                yield Node(
                    "NLM",
                    nlm_id,
                    labels=[self.journal_node_type],
                    data={
                        "name": journal_name,
                        "issn_l": journal_issn_l,
                        "issn_list:string[]": ";".join(issn_list),
                        "wd_id": "WD:" + journal_wd_id,
                    },
                )

    def _get_publisher_nodes(self) -> Iterable[Node]:
        with gzip.open(self.publisher_data_path, 'rt') as fh:
            reader = csv.reader(fh, delimiter='\t')
            for row in reader:
                publisher_wd_id, publisher_name, publisher_isni = row
                yield Node(
                    "ISNI",
                    publisher_isni,
                    labels=[self.publisher_node_type],
                    data={
                        "name": publisher_name,
                        "wd_id": "WD:" + publisher_wd_id,
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
                    target_ns=self.publisher_node_type,
                    target_id=publisher_isni,
                    rel_type="published_by",
                    data={},
                )
