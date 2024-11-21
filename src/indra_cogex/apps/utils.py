import json
import numpy
import logging
import time
from collections import defaultdict
from typing import (
    Any,
    DefaultDict,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
    cast,
    Union,
)

from flask import render_template, request
from indra.assemblers.html.assembler import _format_evidence_text, _format_stmt_text
from indra.statements import Statement
from indra.util.statement_presentation import _get_available_ev_source_counts
from indra_cogex.apps.constants import VUE_SRC_JS, VUE_SRC_CSS, sources_dict
from indra_cogex.apps.curation_cache.curation_cache import Curations
from indra_cogex.apps.proxies import curation_cache
from indralab_auth_tools.auth import resolve_auth

logger = logging.getLogger(__name__)

StmtRow = Tuple[str, str, str, str, str, str]


def count_curations(
    curations: Curations, stmts_by_hash: Dict[int, Statement]
) -> Dict[int, Dict[str, DefaultDict[str, int]]]:
    """Count curations for each statement.

    Parameters
    ----------
    curations :
        An iterable of curation dictionaries.
    stmts_by_hash :
        A dictionary mapping statement hashes to statements.

    Returns
    -------
    :
        A dictionary mapping statement hashes to dictionaries mapping curation
        types to counts.
    """
    correct_tags = ["correct", "act_vs_amt", "hypothesis"]
    cur_counts: Dict[int, Dict[str, DefaultDict[str, int]]] = {}
    for cur in curations:
        stmt_hash = cur["pa_hash"]
        if stmt_hash not in stmts_by_hash:
            print(f"{stmt_hash} not among {list(stmts_by_hash.keys())}")
            continue
        if stmt_hash not in cur_counts:
            cur_counts[stmt_hash] = {
                "this": defaultdict(int),
                "other": defaultdict(int),
            }
        if cur["tag"] in correct_tags:
            cur_tag = "correct"
        else:
            cur_tag = "incorrect"
        if cur["source_hash"] in [
            evid.get_source_hash() for evid in stmts_by_hash[stmt_hash].evidence
        ]:
            cur_source = "this"
        else:
            cur_source = "other"
        cur_counts[stmt_hash][cur_source][cur_tag] += 1
    return cur_counts


def render_statements(
    stmts: List[Statement],
    evidence_counts: Optional[Mapping[int, int]] = None,
    evidence_lookup_time: Optional[float] = None,
    limit: Optional[int] = None,
    curations: Optional[List[Mapping[str, Any]]] = None,
    source_counts_dict: Optional[Mapping[int, Mapping[str, int]]] = None,
    include_db_evidence=True,
    is_proteocentric=False,
    **kwargs,
) -> str:
    """Render INDRA statements.

    Parameters
    ----------
    stmts :
        List of INDRA Statement objects to be rendered
    evidence_counts :
        Dictionary mapping statement hash to total number of evidences
    evidence_lookup_time :
        Time taken to look up evidences in seconds
    limit :
        Maximum number of statements to render
    curations :
        List of curation data dictionaries for the statements
    source_counts_dict :
        Dictionary mapping statement hash to dictionaries of
        source name to source counts
    include_db_evidence :
        Whether to include evidence from databases in the display, by default True
    is_proteocentric :
        Whether the view is protein-centric, enabling specific protein-related features,
        by default False
    kwargs :
        Additional keyword arguments to pass to the template renderer

    Returns
    -------
    str :
        HTML string of the rendered statements
    """

    _, _, user_email = resolve_email()
    remove_medscan = not bool(user_email)

    start_time = time.time()
    formatted_stmts = format_stmts(
        stmts=stmts,
        evidence_counts=evidence_counts,
        limit=limit,
        curations=curations,
        remove_medscan=remove_medscan,
        source_counts_per_hash=source_counts_dict,
    )
    end_time = time.time() - start_time

    if evidence_lookup_time:
        footer = f"Got evidences in {evidence_lookup_time:.2f} seconds. "
    else:
        footer = ""
    footer += f"Formatted {len(formatted_stmts)} statements in {end_time:.2f} seconds."

    response = render_template(
        "data_display/data_display_base.html",
        stmts=formatted_stmts,
        user_email=user_email,
        footer=footer,
        vue_src_js=VUE_SRC_JS,
        vue_src_css=VUE_SRC_CSS,
        sources_dict=sources_dict,
        include_db_evidence=include_db_evidence,
        is_proteocentric=is_proteocentric,
        **kwargs,
    )
    logger.info("Template rendered successfully")
    return response


def format_stmts(
    stmts: Iterable[Statement],
    evidence_counts: Optional[Mapping[int, int]] = None,
    limit: Optional[int] = None,
    curations: Optional[List[Mapping[str, Any]]] = None,
    remove_medscan: bool = True,
    source_counts_per_hash: Optional[Dict[int, Dict[str, int]]] = None,
) -> List[StmtRow]:
    """Format the statements in the way that Patrick's Vue.js components expect.

    Wanted objects:
    - evidence: array of evidence json objects to be passed to <evidence>
    - english: html formatted text
    - hash: as a string
    - sources: source counts
    - total_evidence: total evidence count
    - badges (what is this?) see emmaa_service/api.py::_make_badges() -
        Evidence count:
            {'label': 'evidence', 'num': evid_count, 'color': 'grey',
             'symbol': None, 'title': 'Evidence count for this statement',
             'loc': 'right'},
        Belief:
            {'label': 'belief', 'num': belief, 'color': '#ffc266',
             'symbol': None, 'title': 'Belief score for this statement',
             'loc': 'right'},
        Correct curation count:
            {'label': 'correct_this', 'num': cur_counts['this']['correct'],
             'color': '#28a745', 'symbol':  '\u270E',
             'title': 'Curated as correct in this model'},

    Parameters
    ----------
    stmts :
        An iterable of statements.
    evidence_counts :
        A dictionary mapping statement hashes to evidence counts.
    limit :
        The maximum number of statements to return.
    curations :
        A list of curations.
    remove_medscan :
        Whether to remove MedScan evidences.
    source_counts_per_hash :
        A dictionary mapping statement hashes to source counts for that statement.

    Returns
    -------
    :
        A list of tuples of the form (evidence, english, hash, sources,
        total_evidence, badges).
    """
    if evidence_counts is None:
        evidence_counts = {}
        for stmt in stmts:
            if isinstance(stmt, list):
                # Assuming the hash is the first element and evidence is the second
                stmt_hash, evidence = stmt[0], stmt[1]
                evidence_counts[stmt_hash] = len(evidence)
            else:
                evidence_counts[stmt.get_hash()] = len(stmt.evidence)
    else:
        logger.debug(f"format_stmts input evidence_counts: {evidence_counts}")
    # Make sure statements are sorted by highest evidence counts first
    if all(isinstance(stmt, list) for stmt in stmts):
        stmts = sorted(stmts, key=lambda s: evidence_counts[s[0]], reverse=True)
    else:
        stmts = sorted(stmts, key=lambda s: evidence_counts[s.get_hash()], reverse=True)

    all_pa_hashes: Set[int] = {st.get_hash() for st in stmts}
    if curations is None:
        curations = curation_cache.get_curations(pa_hash=list(all_pa_hashes))
    elif isinstance(curations, list):
        # If curations is already a list, we assume it's in the correct format
        pass
    else:
        # Assuming curations is a Curations object with a get_hash method
        curations = curations.get_curations(pa_hash=list(all_pa_hashes))

    curations = [c for c in curations if c["pa_hash"] in all_pa_hashes]
    cur_dict = defaultdict(list)
    for cur in curations:
        cur_dict[cur["pa_hash"], cur["source_hash"]].append({"error_type": cur["tag"]})

    stmts_by_hash = {st.get_hash(): st for st in stmts}
    cur_counts = count_curations(curations, stmts_by_hash)
    if cur_counts:
        assert isinstance(
            list(cur_counts.keys())[0], (int, numpy.integer)
        ), f"{list(cur_counts.keys())[0]} is not an int"
        key = list(cur_counts.keys())[0]
        assert isinstance(
            list(cur_counts[key].keys())[0], str
        ), f"{list(cur_counts[key].keys())[0]} is not an str"

    stmt_rows = []
    for stmt in stmts:
        if isinstance(stmt, list):
            stmt_hash, evidence = stmt[0], stmt[1]
            row = _stmt_to_row(
                stmts_by_hash[stmt_hash],
                cur_dict=cur_dict,
                cur_counts=cur_counts,
                remove_medscan=remove_medscan,
                source_counts=source_counts_per_hash.get(stmt_hash) if source_counts_per_hash else None,
            )
        else:
            row = _stmt_to_row(
                stmt,
                cur_dict=cur_dict,
                cur_counts=cur_counts,
                remove_medscan=remove_medscan,
                source_counts=source_counts_per_hash.get(stmt.get_hash()) if source_counts_per_hash else None,
            )
        if row is not None:
            stmt_rows.append(row)

    return stmt_rows[:limit] if limit else stmt_rows


def _stmt_to_row(
    stmt: Statement,
    *,
    cur_dict,
    cur_counts,
    remove_medscan: bool = True,
    source_counts: Dict[str, int] = None,
    include_belief_badge: bool = False,
) -> Optional[StmtRow]:
    # Todo: Refactor this function so that evidences can be passed on their
    #  own without having to be passed in as part of the statement.
    ev_array = _format_evidence_text(
        stmt,
        curation_dict=cur_dict,
        correct_tags=["correct", "act_vs_amt", "hypothesis"],
    )

    if remove_medscan:
        ev_array = [e for e in ev_array if e["source_api"] != "medscan"]
        if not ev_array:
            return None

    for ev in ev_array:
        # Translate OrderedDict to dict
        org_json = ev["original_json"]
        ev["original_json"] = dict(org_json)

    english = _format_stmt_text(stmt)
    hash_int = stmt.get_hash()
    if source_counts is None:
        sources = _get_available_ev_source_counts(stmt.evidence)
    else:
        sources = source_counts

    # Calculate the total evidence as the sum of each of the sources' evidences
    total_evidence = sum(sources.values())

    # Remove medscan from the sources count
    if remove_medscan and "medscan" in sources:
        del sources["medscan"]

    badges = [
        {
            "label": "evidence",
            "num": total_evidence,
            "color": "grey",
            "symbol": "",
            "title": "Evidence count for this statement",
            "loc": "right",
        },
    ]
    if include_belief_badge:
        badges.append(
            {
                "label": "belief",
                "num": round(stmt.belief, 2),  # max two sig figs
                "color": "#ffc266",
                "symbol": "",
                "title": "Belief score for this statement",
                "loc": "right",
            },
        )
    if cur_counts and hash_int in cur_counts:
        # Ensure numpy ints are converted to json serializable ints
        num = int(cur_counts[hash_int]["this"]["correct"])
        badges.append(
            {
                "label": "correct_this",
                "num": num,
                "color": "#28a745",
                "symbol": "\u270E",
                "title": f"{num} evidences curated as correct",
                "loc": "left",
            }
        )

    return cast(
        StmtRow,
        tuple(
            json.dumps(e)
            for e in (
                ev_array,
                english,
                str(hash_int),
                sources,
                total_evidence,
                badges,
            )
        ),
    )


def resolve_email():
    user, roles = resolve_auth(dict(request.args))
    email = user.email if user else ""
    return user, roles, email


def get_curated_pa_hashes(
    curations: Optional[List[Mapping[str, Any]]] = None, only_correct: bool = True
) -> Mapping[int, Set[int]]:
    """Get a mapping from statement hashes to evidence hashes."""
    if curations is None:
        curations = curation_cache.get_curation_cache()
    rv = defaultdict(set)
    for curation in curations:
        if not only_correct or curation["tag"] == "correct":
            rv[curation["pa_hash"]].add(curation["source_hash"])
    return dict(rv)


def remove_curated_pa_hashes(
    pa_hashes: Iterable[int],
    curations: Optional[List[Mapping[str, Any]]] = None,
) -> List[int]:
    """Remove all hashes from the list that have already been curated."""
    curated_pa_hashes = get_curated_pa_hashes(curations=curations)
    return [pa_hash for pa_hash in pa_hashes if pa_hash not in curated_pa_hashes]


def remove_curated_statements(
    statements: Iterable[Union[Statement, List]],
    curations: Optional[List[Mapping[str, Any]]] = None,
    include_db_evidence: bool = False,
) -> List[Union[Statement, List]]:
    """Remove all hashes from the list that have already been curated."""
    curated_pa_hashes = get_curated_pa_hashes(curations=curations)

    def should_keep(stmt):
        if isinstance(stmt, list):
            stmt_hash = stmt[0]
            evidence = stmt[1] if len(stmt) > 1 and isinstance(stmt[1], dict) else {}
            # Handle include_db_evidence for list-type statements
            if include_db_evidence and evidence.get('has_database_evidence', False):
                return True
            return stmt_hash not in curated_pa_hashes
        else:
            # Handle include_db_evidence for Statement objects
            if include_db_evidence and hasattr(stmt, 'evidence'):
                if any(getattr(e, 'has_database_evidence', False) for e in stmt.evidence):
                    return True
            return stmt.get_hash() not in curated_pa_hashes

    kept_statements = [stmt for stmt in statements if should_keep(stmt)]
    return kept_statements


def remove_curated_evidences(
    statements: List[Statement],
    curations: Optional[List[Mapping[str, Any]]] = None,
) -> List[Statement]:
    """Remove evidences that are already curated, and if none remain, remove the statement."""
    curated_pa_hashes = get_curated_pa_hashes(curations=curations, only_correct=False)
    rv = []
    removed_statements = 0
    removed_evidences = 0
    for stmt in statements:
        ev_hashes = curated_pa_hashes.get(stmt.get_hash(), set())
        if not ev_hashes:
            rv.append(stmt)
        else:
            pre_count = len(stmt.evidence)
            evidences = [
                evidence
                for evidence in stmt.evidence
                if evidence.get_source_hash() not in ev_hashes
            ]
            removed_evidences += pre_count - len(evidences)
            if evidences:
                stmt.evidence = evidences
                rv.append(rv)
            else:
                removed_statements += 1
    logger.debug(
        f"filtered {removed_evidences} curated evidences and {removed_statements} fully curated statements"
    )
    return rv
