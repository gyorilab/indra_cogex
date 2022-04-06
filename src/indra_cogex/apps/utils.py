import json
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
)

from flask import Response, render_template, request
from indra.assemblers.html.assembler import _format_evidence_text, _format_stmt_text
from indra.sources.indra_db_rest import get_curations
from indra.statements import Statement
from indra.util.statement_presentation import _get_available_ev_source_counts
from indralab_auth_tools.auth import resolve_auth

StmtRow = Tuple[str, str, str, str, str, str]
CurationType = List[Mapping[str, Any]]


def count_curations(
    curations: CurationType, stmts_by_hash: Dict[int, Statement]
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
    cur_counts = {}
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
    **kwargs,
) -> Response:
    """Render INDRA statements."""
    _, _, user_email = resolve_email()

    start_time = time.time()
    formatted_stmts = format_stmts(
        stmts=stmts,
        evidence_counts=evidence_counts,
        limit=limit,
        curations=curations,
    )
    end_time = time.time() - start_time

    if evidence_lookup_time:
        footer = f"Got evidences in {evidence_lookup_time:.2f} seconds. "
    else:
        footer = ""
    footer += f"Formatted {len(formatted_stmts)} statements in {end_time:.2f} seconds."

    return render_template(
        "data_display/data_display_base.html",
        stmts=formatted_stmts,
        user_email=user_email,
        footer=footer,
        **kwargs,
    )


def format_stmts(
    stmts: Iterable[Statement],
    evidence_counts: Optional[Mapping[int, int]] = None,
    limit: Optional[int] = None,
    curations: Optional[List[Mapping[str, Any]]] = None,
) -> List[StmtRow]:
    """Format the statements for display

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

    Returns
    -------
    :
        A list of tuples of the form (evidence, english, hash, sources,
        total_evidence, badges).
    """
    if evidence_counts is None:
        evidence_counts = {}
    else:
        # Make sure statements are sorted by highest evidence counts first
        stmts = sorted(stmts, key=lambda s: evidence_counts[s.get_hash()], reverse=True)

    all_pa_hashes: Set[int] = {st.get_hash() for st in stmts}
    if curations is None:
        curations = get_curations()
    curations = [c for c in curations if c["pa_hash"] in all_pa_hashes]
    cur_dict = defaultdict(list)
    for cur in curations:
        cur_dict[cur["pa_hash"], cur["source_hash"]].append({"error_type": cur["tag"]})

    stmts_by_hash = {st.get_hash(): st for st in stmts}
    cur_counts = count_curations(curations, stmts_by_hash)
    if cur_counts:
        assert isinstance(
            list(cur_counts.keys())[0], int
        ), f"{list(cur_counts.keys())[0]} is not an int"
        key = list(cur_counts.keys())[0]
        assert isinstance(
            list(cur_counts[key].keys())[0], str
        ), f"{list(cur_counts[key].keys())[0]} is not an str"

    stmt_rows = [
        _stmt_to_row(
            stmt,
            cur_dict=cur_dict,
            evidence_counts=evidence_counts,
            cur_counts=cur_counts,
        )
        for stmt in stmts
    ]
    return stmt_rows[:limit] if limit else stmt_rows


def _stmt_to_row(
    stmt: Statement,
    cur_dict,
    evidence_counts,
    cur_counts,
) -> StmtRow:
    ev_array = json.loads(
        json.dumps(
            _format_evidence_text(
                stmt,
                curation_dict=cur_dict,
                correct_tags=["correct", "act_vs_amt", "hypothesis"],
            )
        )
    )
    english = _format_stmt_text(stmt)
    hash_int = stmt.get_hash()
    sources = _get_available_ev_source_counts(stmt.evidence)
    total_evidence = evidence_counts.get(hash_int, len(stmt.evidence))
    badges = [
        {
            "label": "evidence",
            "num": total_evidence,
            "color": "grey",
            "symbol": None,
            "title": "Evidence count for this statement",
            "loc": "right",
        },
        {
            "label": "belief",
            "num": stmt.belief,
            "color": "#ffc266",
            "symbol": None,
            "title": "Belief score for this statement",
            "loc": "right",
        },
    ]
    if cur_counts and hash_int in cur_counts:
        num = cur_counts[hash_int]["this"]["correct"]
        badges.append(
            {
                "label": "correct_this",
                "num": num,
                "color": "#28a745",
                "symbol": "\u270E",
                "title": f"{num} evidences curated as correct",
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
        curations = get_curations()
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
    statements: Iterable[Statement],
    curations: Optional[List[Mapping[str, Any]]] = None,
) -> List[Statement]:
    """Remove all hashes from the list that have already been curated."""
    curated_pa_hashes = get_curated_pa_hashes(curations=curations)
    return [
        statement
        for statement in statements
        if statement.get_hash() not in curated_pa_hashes
    ]


def remove_curated_evidences(
    statements: List[Statement],
    curations: Optional[List[Mapping[str, Any]]] = None,
) -> List[Statement]:
    """Remove evidences that are already curated, and if none remain, remove the statement."""
    curated_pa_hashes = get_curated_pa_hashes(curations=curations, only_correct=False)
    rv = []
    for stmt in statements:
        ev_hashes = curated_pa_hashes.get(stmt.get_hash(), set())
        if not ev_hashes:
            rv.append(stmt)
        else:
            evidences = [
                evidence
                for evidence in stmt.evidence
                if evidence.get_source_hash() not in ev_hashes
            ]
            if evidences:
                stmt.evidence = evidences
                rv.append(rv)
    return rv
