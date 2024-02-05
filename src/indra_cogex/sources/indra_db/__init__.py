# -*- coding: utf-8 -*-

"""Processor for the INDRA database."""

import csv
import gzip
import json
import logging
import os
import pickle
import textwrap
from collections import defaultdict
from itertools import permutations
from pathlib import Path
from typing import Iterable, Optional, Tuple, Union

from indra.literature.pubmed_client import is_retracted
from indra.statements import (
    Agent,
    default_ns_order,
    stmt_from_json,
    Complex,
    Conversion,
)
from indra.util import batch_iter
from indra.sources import SOURCE_INFO
from tqdm import tqdm

from indra_cogex.representation import Node, Relation
from indra_cogex.sources.processor import Processor

from indra_cogex.sources.indra_db.raw_export import stmts_from_json
from indra_cogex.sources.indra_db.locations import (
    belief_scores_pkl_fname,
    processed_stmts_fname,
    unique_stmts_fname,
    source_counts_fname
)
from indra_cogex.sources.pubmed.locations import pmid_year_types_path
from indra_cogex.sources.utils import get_bool
from indra_cogex.util import load_stmt_json_str

logger = logging.getLogger(__name__)


reader_sources = {k for k, v in SOURCE_INFO.items() if v["type"] == "reader"}
db_sources = {k for k, v in SOURCE_INFO.items() if v["type"] == "database"}


# If you don't have the data, run the script in raw_export.py and then in
# assembly.py (both in this directory) to get it.


class DbProcessor(Processor):
    """Processor for the INDRA database."""

    name = "database"
    node_types = ["BioEntity"]

    def __init__(self, dir_path: Union[None, str, Path] = None):
        """Initialize the INDRA database processor.

        Parameters
        ----------
        dir_path :
            The path to the directory containing unique and grounded
            statements as a *.tsv.gz file, source counts as a pickle file and
            belief scores as a pickle file.
        """
        if dir_path is None:
            dir_path = unique_stmts_fname.parent
        elif isinstance(dir_path, str):
            dir_path = Path(dir_path)
        self.stmts_fname = dir_path / unique_stmts_fname.name
        self.source_counts_fname = dir_path / source_counts_fname.name
        self.belief_scores_fname = dir_path / belief_scores_pkl_fname.name

    def get_nodes(self):  # noqa:D102
        # Read the unique statements from the file and yield unique agents
        # The file contains statements that have already been filtered for
        # ungrounded statements, so we can just use the agent list.
        batch_size = 100000
        with gzip.open(self.stmts_fname.as_posix(), "rt") as f:
            reader = csv.reader(f, delimiter="\t")
            seen_agents = set()  # Store ns:id pairs of seen agents

            for batch in tqdm(
                batch_iter(reader, batch_size=batch_size, return_func=list),
                desc="Getting BioEntity nodes",
            ):
                sj_list = [load_stmt_json_str(sjs) for _, sjs in batch]
                stmts = stmts_from_json(sj_list)
                for stmt in stmts:
                    for agent in stmt.real_agent_list():
                        db_ns, db_id = get_ag_ns_id(agent)
                        if db_ns and db_id and (db_ns, db_id) not in seen_agents:
                            yield Node(
                                db_ns, db_id, ["BioEntity"], dict(name=agent.name)
                            )
                            seen_agents.add((db_ns, db_id))

    def get_relations(self, max_complex_members: int = 3):  # noqa:D102
        rel_type = "indra_rel"
        total_count = 0

        # Load the source counts and belief scores into dictionaries
        logger.info("Loading source counts per hash")
        with self.source_counts_fname.open("rb") as f:
            source_counts = pickle.load(f)
        logger.info("Loading belief scores per hash")
        with self.belief_scores_fname.open("rb") as f:
            belief_scores = pickle.load(f)
        logger.info("Loading stmt hash - retraction boolean mapping")
        has_retracted_pmid = {}
        with gzip.open(self.stmts_fname, "rt") as fh:
            reader = csv.reader(fh, delimiter="\t")
            for sh_str, stmt_json_str in tqdm(
                    reader, desc="Reading statements", unit_scale=True
            ):
                stmt_hash = int(sh_str)
                stmt_json = load_stmt_json_str(stmt_json_str)
                evidence = stmt_json["evidence"][0]

                # Get pmid
                pmid = evidence.get("text_refs", {}).get("PMID") or \
                    evidence.get("pmid")
                # If we have a pmid, check if it is retracted.
                # If this is the first time we see this statement hash,
                # set the has_retracted_pmid flag to the retraction status,
                # otherwise, only change it if the pmid is retracted.
                if pmid is not None:
                    if stmt_hash not in has_retracted_pmid:
                        has_retracted_pmid[stmt_hash] = is_retracted(pmid)
                    else:
                        # Run OR on the current value and the new value
                        has_retracted_pmid[stmt_hash] |= is_retracted(pmid)

        hashes_yielded = set()
        with gzip.open(self.stmts_fname, "rt") as fh:
            reader = csv.reader(fh, delimiter="\t")
            for sh_str, stmt_json_str in tqdm(reader, desc="Reading statements"):
                stmt_hash = int(sh_str)
                if stmt_hash in hashes_yielded:
                    continue

                try:
                    source_count = source_counts[stmt_hash]
                    belief = belief_scores[stmt_hash]
                except KeyError:
                    # NOTE: this should not happen if files are generated
                    # properly and are up to date.
                    logger.warning(
                        f"Could not find source count or belief score for "
                        f"statement hash {stmt_hash}. Are the source files updated?"
                    )
                    continue
                stmt_json = load_stmt_json_str(stmt_json_str)
                if stmt_json["evidence"][0]["source_api"] == "medscan":
                    stmt_json["evidence"] = []

                # Set belief in the statement json
                stmt_json["belief"] = belief

                data = {
                    "stmt_hash:int": stmt_hash,
                    "source_counts:string": json.dumps(source_count),
                    "evidence_count:int": sum(source_count.values()),
                    "stmt_type:string": stmt_json["type"],
                    "belief:float": belief,
                    "stmt_json:string": json.dumps(stmt_json),
                    "has_retracted_evidence:boolean": get_bool(
                        has_retracted_pmid.get(stmt_hash)
                    ),
                    "has_database_evidence:boolean": get_bool(set(source_count) & db_sources),
                    "has_reader_evidence:boolean": get_bool(set(source_count) & reader_sources),
                    "medscan_only:boolean": get_bool(set(source_count) == {"medscan"}),
                    "sparser_only:boolean": get_bool(set(source_count) == {"sparser"}),
                }

                # Get the agents from the statement
                stmt = stmt_from_json(stmt_json)
                agents = stmt.real_agent_list()

                # We skip Conversions
                if isinstance(stmt, Conversion):
                    continue

                # If we don't have at least 2 real agents, we skip it
                if len(agents) < 2:
                    continue

                # We skip any Statements that have ungrounded Agents
                agent_groundings = [get_ag_ns_id(agent) for agent in agents]
                if any(
                    ag_ns is None or ag_id is None for ag_ns, ag_id in agent_groundings
                ):
                    continue

                # We need special handling for Complexes
                if isinstance(stmt, Complex):
                    if len(agents) > max_complex_members:
                        continue
                    for (ns_a, id_a), (ns_b, id_b) in permutations(agent_groundings, 2):
                        yield Relation(ns_a, id_a, ns_b, id_b, rel_type, data)
                        total_count += 1
                # Otherwise we expect this to be a well behaved binary statement
                # that we can simply turn into a relation
                elif len(agents) == 2:
                    yield Relation(
                        *agent_groundings[0], *agent_groundings[1], rel_type, data
                    )
                    total_count += 1
                else:
                    continue

                hashes_yielded.add(stmt_hash)

        logger.info(
            f"Got {total_count} total relations from {len(hashes_yielded)} unique statements"
        )


class EvidenceProcessor(Processor):
    name = "indra_db_evidence"
    node_types = ["Evidence", "Publication"]

    def __init__(self):
        """Initialize the Evidence processor"""
        self.stmt_fname = processed_stmts_fname
        self._stmt_id_pmid_links = {}
        # Check if files exist without loading them
        if not self.stmt_fname.exists():
            raise FileNotFoundError(f"No such file: {self.stmt_fname}")

    def get_nodes(self, num_rows: Optional[int] = None) -> Iterable[Node]:
        """Get INDRA Evidence and Publication nodes"""
        # First, we need to figure out which Statements were actually
        # selected in the DbProcessor and only include evidences for those.
        included_hashes = set()
        logger.info("Loading relevant statement hashes...")
        with gzip.open(DbProcessor.edges_path, "rt") as fh:
            reader = csv.reader(fh, delimiter="\t")
            header = next(reader)
            hash_idx = header.index("stmt_hash:int")
            for row in reader:
                included_hashes.add(int(row[hash_idx]))

        # Create pubmed source files if they don't exist
        if not pmid_year_types_path.exists():
            from indra_cogex.sources.pubmed import process_mesh_xml_to_csv
            process_mesh_xml_to_csv()

        # Load pmid -> year, pubtypes mapping (assumes pubmed source files
        # exist)
        with gzip.open(pmid_year_types_path, "rt") as fh:
            pmid_years_pubtypes = {
                pmid: (year, json.loads(types))
                for pmid, year, types in csv.reader(fh, delimiter="\t")
            }

        # Loop the grounded statements and get the evidence w text refs
        logger.info("Looping statements from statements file")
        with gzip.open(self.stmt_fname.as_posix(), "rt") as fh:
            # TODO test whether this is a reasonable size
            batch_size = 100000
            # TODO get number of batches from the total number of statements
            #  rather than hardcoding
            total = num_rows // batch_size + 1 if num_rows else 600
            reader = csv.reader(fh, delimiter="\t")
            yield_index = 0
            yielded_pmid = set()
            for batch in tqdm(
                batch_iter(reader, batch_size=batch_size, return_func=list),
                total=total,
            ):
                node_batch = []
                for stmt_hash_str, stmt_json_str in batch:
                    stmt_hash = int(stmt_hash_str)
                    if stmt_hash not in included_hashes:
                        continue
                    stmt_json = load_stmt_json_str(stmt_json_str)

                    # Loop all evidences
                    # NOTE: there should be a single evidence for each
                    # statement so looping is probably not necessary
                    evidence_list = stmt_json["evidence"]
                    for evidence in evidence_list:
                        tr = evidence.get("text_refs", {})
                        pmid = tr.get("PMID") or evidence.get("pmid")

                        # Only yield Pubmed nodes if we have PMID and it
                        # hasn't already been used
                        if pmid is not None and pmid not in yielded_pmid:
                            year, pubtypes = pmid_years_pubtypes.get(pmid, (None, []))
                            pubtypes_str = ";".join(pubtypes) or None
                            # If there are text refs, use them
                            if tr.get("PMID"):
                                pubmed_node = Node(
                                    db_ns="PUBMED",
                                    db_id=pmid,
                                    labels=["Publication"],
                                    data={
                                        "trid": tr.get("TRID"),
                                        "pmcid": tr.get("PMCID"),
                                        "doi": tr.get("DOI"),
                                        "pii": tr.get("PII"),
                                        "url": tr.get("URL"),
                                        "manuscript_id": tr.get("MANUSCRIPT_ID"),
                                        "year:int": year,
                                        "publication_type:string[]": pubtypes_str,
                                        "retracted:boolean": get_bool(
                                            is_retracted(pmid)
                                        )
                                    },
                                )
                            # Otherwise, just make a node with the evidence PMID
                            elif evidence.get("pmid"):
                                pubmed_node = Node(
                                    db_ns="PUBMED",
                                    db_id=pmid,
                                    labels=["Publication"],
                                    data={
                                        "year:int": year,
                                        "publication_type:string[]": pubtypes_str,
                                        "retracted:boolean": get_bool(
                                            is_retracted(pmid)
                                        )
                                    },
                                )

                            # Add Publication node to batch if it was created
                            node_batch.append(pubmed_node)
                            yielded_pmid.add(pmid)

                        # Add Evidence -> Publication mapping if there is a PMID
                        if pmid is not None:
                            self._stmt_id_pmid_links[yield_index] = pmid

                        # TODO: Add node for any context associated with the evidence
                        # Issues to resolve
                        # - context db_refs are often unnormalized and proper normalization
                        #   would require mappings not available in the INDRA BioOntology
                        #   e.g., {'name': None, 'db_refs': {'TAXONOMY': '9606'}}
                        #   would require TAXONOMY -> MESH mapping + name standardization
                        # similarly, {'name': None, 'db_refs': {'UPLOC': 'SL-0310'}}
                        # would require using UPLOC -> GO mappings to normalize
                        # Some keys appear to be invalid e.g., CELLOSAURUS
                        #  is not the standard CVCL INDRA normally expects.

                        # Always add the Evidence node for this evidence
                        node_batch.append(
                            Node(
                                db_ns="indra_evidence",
                                db_id=str(yield_index),
                                labels=["Evidence"],
                                data={
                                    # Check retractions if there is a PMID
                                    "retracted:boolean": get_bool(
                                        is_retracted(pmid) if pmid else False
                                    ),
                                    "evidence:string": json.dumps(evidence),
                                    "stmt_hash:int": stmt_hash,
                                    "source_api:string": evidence["source_api"],
                                },
                            )
                        )
                        yield_index += 1

                yield node_batch

    def get_relations(self):
        for yield_index, pmid in self._stmt_id_pmid_links.items():
            yield Relation(
                "indra_evidence", yield_index, "PUBMED", pmid, "has_citation"
            )

    def _dump_nodes(self) -> Path:
        # This overrides the default implementation in Processor because
        # we want to process Evidence nodes in batches
        paths_by_type = {}
        nodes_by_type = defaultdict(list)
        # Process Evidence and Publication nodes differently
        evid_node_type = "Evidence"
        pmid_node_type = "Publication"
        nodes_path, nodes_indra_path, sample_path = self._get_node_paths(evid_node_type)
        paths_by_type[evid_node_type] = nodes_path
        # From each batch get the nodes by type but only process Evidence nodes at the moment
        for bidx, nodes in enumerate(self.get_nodes()):
            logger.info(f"Processing batch {bidx}")
            for node in nodes:
                nodes_by_type[node.labels[0]].append(node)
            # We'll append all batches to a single tsv file
            write_mode = "wt"
            if bidx > 0:
                sample_path = None
                write_mode = "at"
            nodes = sorted(
                nodes_by_type[evid_node_type], key=lambda x: (x.db_ns, x.db_id)
            )
            self._dump_nodes_to_path(nodes, nodes_path, sample_path, write_mode)
            # Remove Evidence nodes batch because we don't need to keep them in memory,
            # keep the Publication nodes since we haven't processed them yet
            nodes_by_type[evid_node_type] = []
        # Now process the Publication nodes
        nodes_path, nodes_indra_path, sample_path = self._get_node_paths(pmid_node_type)
        paths_by_type[pmid_node_type] = nodes_path
        with open(nodes_indra_path, "wb") as fh:
            pickle.dump(nodes, fh)
        nodes = sorted(nodes_by_type[pmid_node_type], key=lambda x: (x.db_ns, x.db_id))
        nodes_by_type[pmid_node_type] = nodes
        self._dump_nodes_to_path(nodes, nodes_path, sample_path)
        return paths_by_type, dict(nodes_by_type)

    @classmethod
    def _get_node_paths(cls, node_type: str) -> Path:
        if node_type == "Publication":
            return super()._get_node_paths(node_type)
        elif node_type == "Evidence":
            return (
                cls.module.join(name=f"nodes_{node_type}.tsv.gz"),
                None,
                cls.module.join(name=f"nodes_{node_type}_sample.tsv"),
            )


def get_ag_ns_id(ag: Agent) -> Tuple[str, str]:
    """Return a namespace, identifier tuple for a given agent.

    Parameters
    ----------
    ag :
        The agent to get the namespace and identifier for.

    Returns
    -------
    :
        A namespace, identifier tuple.
    """
    for ns in default_ns_order:
        if ns in ag.db_refs:
            return ns, ag.db_refs[ns]
    return None, None


def load_text_refs_for_reading_dict(fname: str):
    text_refs = {}
    for line in tqdm(
        gzip.open(fname, "rt", encoding="utf-8"),
        desc="Processing text refs for readings into a lookup dictionary",
    ):
        ids = line.strip().split("\t")
        id_names = ["TRID", "PMID", "PMCID", "DOI", "PII", "URL", "MANUSCRIPT_ID"]
        d = {}
        rid = ids[0]
        for id_name, id_val in zip(id_names, ids[1:]):
            if id_val != "\\N":
                d[id_name] = id_val
        text_refs[rid] = d
    return text_refs


def ensure_statements_with_evidences(fname):
    if os.path.exists(fname):
        logger.info(f"Found existing statements with evidences in {fname}")
        return
    from indra_db import get_ro

    db = get_ro("primary")
    os.environ["PGPASSWORD"] = db.url.password
    logger.info(f"Dumping statements with evidences into {fname}")
    command = textwrap.dedent(
        f"""
        psql -d {db.url.database} -h {db.url.host} -U {db.url.username}
        -c "COPY (SELECT id, reading_id, mk_hash, encode(raw_json::bytea, 'escape'),
        encode(pa_json::bytea, 'escape') FROM readonly.fast_raw_pa_link) TO STDOUT"
        | gzip > {fname}
    """
    ).replace("\n", " ")
    os.system(command)


def ensure_text_refs_for_reading(fname):
    if os.path.exists(fname):
        logger.info(f"Found existing text refs for reading in {fname}")
        return
    from indra_db import get_ro

    db = get_ro("primary")
    os.environ["PGPASSWORD"] = db.url.password
    logger.info(f"Dumping text refs for reading into {fname}")
    command = textwrap.dedent(
        f"""
        psql -d {db.url.database} -h {db.url.host} -U {db.url.username}
        -c "COPY (SELECT rid, trid, pmid, pmcid, doi, pii, url, manuscript_id 
        FROM readonly.reading_ref_link) TO STDOUT"
        | gzip > {fname}
    """
    ).replace("\n", " ")
    os.system(command)
