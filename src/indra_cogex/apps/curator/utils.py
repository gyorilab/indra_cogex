"""Utilities for the curation blueprint."""

from collections import Counter, defaultdict
from typing import Any, List, Mapping

from indra.sources.indra_db_rest import get_curations

from indra_cogex.client import Neo4jClient, autoclient

__all__ = [
    "get_conflict_evidence_counts",
    "unfinished",
    "Curation",
]

Curation = Mapping[str, Any]


@autoclient()
def get_conflict_evidence_counts(
    *, curations: List[Curation] = None, client: Neo4jClient
) -> Mapping[int, int]:
    """Get hashes of statements whose curations need resolving."""
    if curations is None:
        curations = get_curations()
    stmt_hash_to_counter = _group_curations(curations)
    query = f"""\
        MATCH (:BioEntity)-[r:indra_rel]->(:BioEntity)
        WHERE
            r.stmt_hash IN {sorted(stmt_hash_to_counter)!r}
        RETURN r.stmt_hash, r.evidence_count
    """
    return {
        stmt_hash: evidence_count
        for stmt_hash, evidence_count in client.query_tx(query)
        if unfinished(
            correct=stmt_hash_to_counter[stmt_hash][True],
            incorrect=stmt_hash_to_counter[stmt_hash][False],
            evidences=evidence_count,
        )
    }


def _group_curations(curations: List[Curation]) -> Mapping[int, Counter]:
    stmt_hash_to_counter = defaultdict(Counter)
    for curation in curations:
        stmt_hash = curation.get("pa_hash")
        if not stmt_hash:
            continue
        positive = curation["tag"] == "correct"
        stmt_hash_to_counter[stmt_hash][positive] += 1
    return dict(stmt_hash_to_counter)


def unfinished(correct: int, incorrect: int, evidences: int) -> bool:
    """Decide if curation should continue on a given statement
    based on previous curation counts.

    Parameters
    ----------
    correct :
        The number of evidences for the statement that have been
        previously curated as correct
    incorrect :
        The number of evidences for the statement that have been
        previously curated as incorrect
    evidences :
        The number of evidences for the statement

    Returns
    -------
    :
        If curation should continue on the given statement
    """
    if incorrect + correct == evidences:
        # All evidences have been curated
        return False
    if incorrect == 0 and correct == 0:
        # No evidences have been curated, so keep curating
        return True
    if incorrect == 0 and correct > 0:
        # At least one evidence has been curated as correct and none as incorrect
        return False
    if correct == 0 and incorrect > 5:
        # Several evidences have been curated as incorrect and none as correct
        # At a certain point, it's better to just stop curating.
        return False
    if incorrect > 1 and correct > (incorrect // 2):
        # if there are a lot of incorrect, you should get at least half as
        # many correct curations before moving on
        return False
    return True
