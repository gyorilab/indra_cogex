"""Metabolomic set analysis utilities."""

import itertools as itt
from collections import defaultdict
from functools import lru_cache
from textwrap import dedent
from typing import Iterable, Mapping, Optional, Set, Tuple

import pandas as pd

from indra_cogex.client.enrichment.discrete import _do_ora
from indra_cogex.client.neo4j_client import Neo4jClient

__all__ = [
    "get_metabolomics_sets",
    "EXAMPLE_CHEBI_IDS",
    "EXAMPLE_CHEBI_CURIES",
    "metabolomics_ora",
]


def _minimum_evidence_helper(
    minimum_evidence_count: Optional[float] = None, name: str = "r"
) -> str:
    if minimum_evidence_count is None or minimum_evidence_count == 1:
        return ""
    return f"AND {name}.evidence_count >= {minimum_evidence_count}"


def _minimum_belief_helper(
    minimum_belief: Optional[float] = None, name: str = "r"
) -> str:
    if minimum_belief is None or minimum_belief == 0.0:
        return ""
    return f"AND {name}.belief >= {minimum_belief}"


@lru_cache()
def get_metabolomics_sets(
    *,
    minimum_evidence_count: Optional[float] = None,
    minimum_belief: Optional[float] = None,
    client: Neo4jClient,
) -> Mapping[Tuple[str, str], Set[str]]:
    """Get a mapping of EC codes to ChEBI local identifiers that it increases/activates.

    Arguments
    ---------
    minimum_evidence_count :
        The minimum number of evidences for a relationship to count it as a regulator.
        Defaults to 1 (i.e., cutoff not applied.
    minimum_belief :
        The minimum belief for a relationship to count it as a regulator.
        Defaults to 0.0 (i.e., cutoff not applied).
    client :
        The Neo4j client.

    Returns
    -------
    : A dictionary of EC codes to set of ChEBI identifiers
    """
    evidence_line = _minimum_evidence_helper(minimum_evidence_count)
    belief_line = _minimum_belief_helper(minimum_belief)
    query = dedent(
        f"""\
    MATCH
        (enzyme:BioEntity)-[:xref]-(family:BioEntity)-[r:indra_rel]->(chemical:BioEntity)
    WHERE
        enzyme.id STARTS WITH "ec-code"
        and family.id STARTS WITH "fplx"
        and chemical.id STARTS WITH "chebi"
        and r.stmt_type in ["Activation", "IncreaseAmount"]
        {evidence_line}
        {belief_line}
    RETURN
        enzyme.id, family.name, collect(chemical.id)
    UNION ALL
    MATCH
        (enzyme:BioEntity)-[:xref]-(family:BioEntity)<-[:isa|partof]-(gene:BioEntity)-[r:indra_rel]->(chemical:BioEntity)
    WHERE
        enzyme.id STARTS WITH "ec-code"
        and family.id STARTS WITH "fplx"
        and chemical.id STARTS WITH "chebi"
        and r.stmt_type in ["Activation", "IncreaseAmount"]
        {evidence_line}
        {belief_line}
    RETURN
        enzyme.id, family.name, collect(chemical.id)
    """
    )
    rv = defaultdict(set)
    for ec_curie, ec_name, chebi_curies in client.query_tx(query):
        ec_code = ec_curie.split(":", 1)[1]
        rv[ec_code, ec_name].update(
            {chebi_curie.split(":", 1)[1] for chebi_curie in chebi_curies}
        )
    return dict(rv)


def _sum_values(d):
    return len(set(itt.chain.from_iterable(d.values())))


def metabolomics_ora(
    *, client: Neo4jClient, chebi_ids: Iterable[str], **kwargs
) -> pd.DataFrame:
    """Calculate over-representation on all metabolites."""
    curie_to_target_sets = get_metabolomics_sets(client=client)
    count = _sum_values(curie_to_target_sets)
    return _do_ora(curie_to_target_sets, query=chebi_ids, count=count, **kwargs)


#: Various alcohol dehydrogenase products
EXAMPLE_CHEBI_IDS = [
    "15366",  # acetic acid
    "15343",  # acetaldehyde
    "16995",  # oxalic acid
    "16842",  # formaldehyde
]

EXAMPLE_CHEBI_CURIES = [f"CHEBI:{i}" for i in EXAMPLE_CHEBI_IDS]


def _main():
    from tabulate import tabulate

    client = Neo4jClient()
    results = get_metabolomics_sets(
        client=client, minimum_belief=0.3, minimum_evidence_count=2
    )
    print("number of enzymes", len(results))
    print("number of metabolites", _sum_values(results))
    print(
        tabulate(
            (
                (
                    ec_code,
                    name,
                    sorted(f"https://bioregistry.io/chebi:{c}" for c in chebi_ids),
                )
                for (ec_code, name), chebi_ids in results.items()
            )
        )
    )


if __name__ == "__main__":
    _main()
