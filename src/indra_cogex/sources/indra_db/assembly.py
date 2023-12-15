import csv
import gzip
import logging
import math
import pickle
import itertools
from pathlib import Path
from typing import List, Set, Tuple, Optional

import networkx as nx
import numpy as np
import tqdm
import pystow
from collections import defaultdict, Counter

from indra.belief import BeliefEngine
from indra.util import batch_iter
from indra.ontology.bio import bio_ontology
from indra.statements import Statement, Evidence
from indra.statements.io import stmt_from_json
from indra.preassembler import Preassembler

from indra_cogex.sources.indra_db.raw_export import (
    unique_stmts_fname,
    source_counts_fname,
)
from indra_cogex.util import load_stmt_json_str

StmtList = List[Statement]

base_folder = pystow.module("indra", "db")
refinements_fname = base_folder.join(name="refinements.tsv.gz")
belief_scores_pkl_fname = base_folder.join(name="belief_scores.pkl")
refinement_cycles_fname = base_folder.join(name="refinement_cycles.pkl")


logger = logging.getLogger(__name__)


def get_refinement_graph() -> nx.DiGraph:
    global cycles_found
    """Get refinement pairs as: (more specific, less specific)

    The evidence from the more specific statement is included in the less
    specific statement

    Step 1, alternative:

    Open two CSV readers for the unique_statements.tsv.gz and then move them
    forward in batches to cover all combinations of Statement batches.

    - First batch of Stmts, internal refinement finding
    - First batch (first reader) x Second batch (second reader)
    - First batch (first reader) x Third batch (second reader)
    - ...
    - One before last batch (first reader) x Last batch (second reader)
    ---> Giant list of refinement relation pairs (hash1, hash2)
    
    Put the pairs in a networkx DiGraph
    """
    # Loop statements: the outer index runs all batches while the inner index
    # runs outer index < inner index <= num_batches. This way the outer
    # index runs the "diagonal" of the combinations while the inner index runs
    # the upper triangle of the combinations.

    # Open two csv readers to the same file
    if not refinements_fname.exists():
        logger.info("Calculating refinements")
        refinements = set()
        # This takes ~10-11 hours to run
        with gzip.open(unique_stmts_fname, "rt") as fh1:
            reader1 = csv.reader(fh1, delimiter="\t")
            for outer_batch_ix in tqdm.tqdm(
                range(num_batches), total=num_batches, desc="Calculating refinements"
            ):
                logger.info("Loading statements for outer batch %s" % outer_batch_ix)
                # read in a batch from the first reader
                stmts1 = []
                for _ in range(batch_size):
                    try:
                        _, sjs = next(reader1)
                        stmt = stmt_from_json(
                            load_stmt_json_str(sjs, remove_evidence=True)
                        )
                        stmts1.append(stmt)
                    except StopIteration:
                        break

                # Get refinements for the i-th batch with itself
                refinements |= get_related(stmts1)

                # Loop batches from second reader, starting at outer_batch_ix + 1
                with gzip.open(unique_stmts_fname, "rt") as fh2:
                    reader2 = csv.reader(fh2, delimiter="\t")
                    batch_iterator = batch_iter(reader2, batch_size=batch_size)
                    # Note: first argument is the start index, second is
                    # the stop index, but if None is used, it will iterate
                    # until possible
                    batch_iterator = itertools.islice(
                        batch_iterator, outer_batch_ix + 1, None
                    )

                    # Loop the batches
                    for inner_batch_idx, batch in enumerate(batch_iterator):
                        logger.info(
                            "Loading statements for inner batch %s"
                            % (outer_batch_ix + inner_batch_idx + 1)
                        )
                        stmts2 = []

                        # Loop the statements in the batch
                        for _, sjs in batch:
                            try:
                                stmt = stmt_from_json(
                                    load_stmt_json_str(sjs,
                                                       remove_evidence=True)
                                )
                                stmts2.append(stmt)
                            except StopIteration:
                                break

                        # Get refinements for the i-th batch with the j-th batch
                        refinements |= get_related_split(stmts1, stmts2)

        # Write out the refinements as a gzipped TSV file
        with gzip.open(refinements_fname.as_posix(), "wt") as f:
            tsv_writer = csv.writer(f, delimiter="\t")
            tsv_writer.writerows(refinements)
    else:
        logger.info(
            f"Loading refinements from existing file {refinements_fname.as_posix()}"
        )
        with gzip.open(refinements_fname.as_posix(), "rt") as f:
            tsv_reader = csv.reader(f, delimiter="\t")

            # Each line is a refinement pair of two Statement hashes as ints
            refinements = {(int(h1), int(h2)) for h1, h2 in tsv_reader}

    # Perform sanity check on the refinements
    logger.info("Checking refinements")
    sample_stmts = sample_unique_stmts(n_rows=num_rows)
    sample_refinements = get_related([s for _, s in sample_stmts])
    assert sample_refinements.issubset(refinements), (
        f"Refinements are not a subset of the sample. Sample contains "
        f"{len(sample_refinements - refinements)} refinements not in "
        f"the full set."
    )

    logger.info("Checking refinements for cycles")
    ref_graph = nx.DiGraph(refinements)
    try:
        cycles = nx.find_cycle(ref_graph)
        cycles_found = True
    except nx.NetworkXNoCycle:
        logger.info("No cycles found in the refinements")
        cycles = None
        cycles_found = False

    # If cycles are found, save them to a file for later inspection
    if cycles_found and cycles is not None:
        logger.warning(
            f"Found cycles in the refinement graph, dumping to {refinement_cycles_fname.as_posix()}"
        )
        with refinement_cycles_fname.open("wb") as f:
            pickle.dump(obj=cycles, file=f)
        cycles_found = True

    return ref_graph


def get_related(stmts: StmtList) -> Set[Tuple[int, int]]:
    stmts_by_type = defaultdict(list)
    for stmt in stmts:
        stmts_by_type[stmt.__class__.__name__].append(stmt)
    refinements = set()
    for _, stmts_this_type in stmts_by_type.items():
        refinements |= pa._generate_relation_tuples(stmts_this_type)
    return refinements


def get_related_split(stmts1: StmtList, stmts2: StmtList) -> Set[Tuple[int, int]]:
    stmts_by_type1 = defaultdict(list)
    stmts_by_type2 = defaultdict(list)
    for stmt in stmts1:
        stmts_by_type1[stmt.__class__.__name__].append(stmt)
    for stmt in stmts2:
        stmts_by_type2[stmt.__class__.__name__].append(stmt)
    refinements = set()
    for stmt_type, stmts_this_type1 in stmts_by_type1.items():
        stmts_this_type2 = stmts_by_type2.get(stmt_type)
        if not stmts_this_type2:
            continue
        refinements |= pa._generate_relation_tuples(
            stmts_this_type1 + stmts_this_type2, split_idx=len(stmts_this_type1) - 1
        )
    return refinements


def sample_unique_stmts(
    num: int = 100000, n_rows: Optional[int] = None
) -> List[Tuple[int, Statement]]:
    """Return a random sample of Statements from unique_statements.tsv.gz

    Parameters
    ----------
    num :
        Number of Statements to return
    n_rows :
        The number of rows in the file. If not provided, the file is read in
        its entirety first to determine the number of rows.

    Returns
    -------
    :
        A list of tuples of the form (hash, Statement)
    """
    if n_rows is None:
        logger.info("Counting lines...")
        with gzip.open(unique_stmts_fname.as_posix(), "rt") as f:
            reader = csv.reader(f, delimiter="\t")
            n_rows = sum(1 for _ in reader)

    # Generate a random sample of line indices
    logger.info(f"Sampling {num} unique statements from a total of {n_rows}")
    indices = np.random.choice(n_rows, num, replace=False)
    stmts = []
    t = tqdm.tqdm(total=num, desc="Sampling statements")
    with gzip.open(unique_stmts_fname, "rt") as f:
        reader = csv.reader(f, delimiter="\t")
        for index, (sh, sjs) in enumerate(reader):
            if index in indices:
                stmts.append((int(sh), stmt_from_json(load_stmt_json_str(sjs))))
                t.update()
                if len(stmts) == num:
                    break

    t.close()
    return stmts


def belief_calc(
        refinements_graph: nx.DiGraph,
        num_batches: int,
        batch_size: int,
        unique_stmts_path: Path = unique_stmts_fname,
        belief_scores_pkl_path: Path = belief_scores_pkl_fname,
        source_counts_path: Path = source_counts_fname,
):
    # Belief Calculations
    """
    Belief calculation idea:
    - Load refinements from dump -> we should probably make a networkx graph
      - that will allow us to call e.g., "descendants" or "ancestors" to get
        hashes of statements that refine a given statement
    - Load source counts from dump
    - Iterate over each unique statement
      - Find all the statements that refine it
      - Add up all the source counts for the statement itself and the statements
        that refine it
      - Pass mock evidences based on source counts (or is there an API directly
        for source counts??) to INDRA's Belief Engine to get a belief score
    - Dump dict of stmt_hash: belief score values

    Todo: bring annotations in to the evidence objects passed to the Belief Engine
    """
    # The refinement set is a set of pairs of hashes, with the *first hash
    # being more specific than the second hash*, i.e. the evidence for the
    # first should be included in the evidence for the second
    #
    # The BeliefEngine expects the refinement graph to be a directed graph,
    # with edges pointing from *more specific* to *less specific* statements
    # (see docstring of indra.belief.BeliefEngine)
    #
    # => The edges represented by the refinement set are the *same* as the
    # edges expected by the BeliefEngine.

    # Initialize a belief engine
    logger.info("Initializing belief engine")
    be = BeliefEngine(refinements_graph=refinements_graph)

    # Load the source counts
    logger.info("Loading source counts")
    with source_counts_path.open("rb") as fh:
        source_counts = pickle.load(fh)

    # Store hash: belief score
    belief_scores = {}

    def _get_support_evidence_for_stmt(stmt_hash: int) -> List[Evidence]:
        # Find all the statements that refine the current
        # statement, i.e. all the statements that are more
        # specific than the current statement => look for ancestors
        # then add up all the source counts for the statement
        # itself and the statements that refine it
        summed_source_counts = Counter(source_counts[stmt_hash])

        # If there are refinements, add them to the source counts
        if stmt_hash in refinements_graph.nodes():
            refiner_hashes = nx.ancestors(refinements_graph, stmt_hash)
            for refiner_hash in refiner_hashes:
                summed_source_counts += Counter(source_counts[refiner_hash])

        # Mock evidence - todo: add annotations?
        # Add evidence objects for each source's count and each source
        ev_list = []
        for source, count in summed_source_counts.items():
            for _ in range(count):
                ev_list.append(Evidence(source_api=source))
        return ev_list

    def _add_belief_scores_for_batch(batch: List[Tuple[int, Statement]]):
        # Belief calculation for this batch
        hashes, stmt_list = zip(*batch)
        be.set_prior_probs(statements=stmt_list)
        for sh, st in zip(hashes, stmt_list):
            belief_scores[sh] = st.belief

    # Iterate over each unique statement
    # Takes ~30-40 minutes
    with gzip.open(unique_stmts_path.as_posix(), "rt") as fh:
        reader = csv.reader(fh, delimiter="\t")

        for _ in tqdm.tqdm(range(num_batches), desc="Calculating belief"):
            stmt_batch = []
            for _ in range(batch_size):
                try:
                    stmt_hash_string, statement_json_string = next(reader)
                    statement = stmt_from_json(
                        load_stmt_json_str(
                            statement_json_string, remove_evidence=True
                        )
                    )
                    this_hash = int(stmt_hash_string)
                    statement.evidence = _get_support_evidence_for_stmt(this_hash)
                    stmt_batch.append((this_hash, statement))

                except StopIteration:
                    break

            _add_belief_scores_for_batch(stmt_batch)

    # Dump the belief scores
    with belief_scores_pkl_path.open("wb") as fo:
        pickle.dump(belief_scores, fo)


if __name__ == "__main__":
    required = [source_counts_fname, unique_stmts_fname]
    if not unique_stmts_fname.exists() or not source_counts_fname.exists():
        raise ValueError(
            f"Missing one or both of the required files: "
            f"{', '.join(r.as_posix() for r in required)}"
        )

    # Global variables
    bio_ontology.initialize()
    bio_ontology._build_transitive_closure()
    pa = Preassembler(bio_ontology)
    batch_size = int(1e6)

    # Count lines in unique statements file
    logger.info(f"Counting lines in {unique_stmts_fname.as_posix()}")
    with gzip.open(unique_stmts_fname.as_posix(), "rt") as fh:
        csv_reader = csv.reader(fh, delimiter="\t")
        num_rows = sum(1 for _ in csv_reader)

    num_batches = math.ceil(num_rows / batch_size)

    cycles_found = False
    refinement_graph = get_refinement_graph()

    # Step 2: Calculate belief scores
    if cycles_found:
        logger.info(
            f"Refinement graph stored in variable 'refinement_graph', "
            f"edges saved to {refinements_fname.as_posix()}"
            f"and cycles saved to {refinement_cycles_fname.as_posix()}"
        )
    else:
        belief_calc(refinement_graph,
                    batch_size=batch_size,
                    num_batches=num_batches)
