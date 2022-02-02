"""Metabolomic set analysis utilities."""

import itertools as itt
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


def minimum_evidence_helper(
    minimum_evidence_count: Optional[float] = None, name: str = "r"
) -> str:
    if minimum_evidence_count is None or minimum_evidence_count == 1:
        return ""
    return f"AND {name}.evidence_count >= {minimum_evidence_count}"


def minimum_belief_helper(
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

    Returns
    -------
    : A dictionary of EC codes to set of ChEBI identifiers
    """
    evidence_line = minimum_evidence_helper(minimum_evidence_count)
    belief_line = minimum_belief_helper(minimum_belief)
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
        enzyme.id, enzyme.name, collect(chemical.id)
    LIMIT 5;
    """
    )
    return {
        (ec_curie.split(":", 1)[1], ec_name): {
            chebi_curie.split(":", 1)[1] for chebi_curie in chebi_curies
        }
        for ec_curie, ec_name, chebi_curies in client.query_tx(query)
    }


def metabolomics_ora(
    *, client: Neo4jClient, chebi_ids: Iterable[str], **kwargs
) -> pd.DataFrame:
    """Calculate over-representation on all metabolites."""
    curie_to_target_sets = get_metabolomics_sets(client=client)
    count = len(set(itt.chain.from_iterable(curie_to_target_sets.values())))
    return _do_ora(curie_to_target_sets, query=chebi_ids, count=count, **kwargs)


#: Coming from EC search1.14.19.1
EXAMPLE_CHEBI_IDS = [
    "15756",
    "16196",
]

EXAMPLE_CHEBI_CURIES = [f"CHEBI:{i}" for i in EXAMPLE_CHEBI_IDS]


def main():
    client = Neo4jClient()
    results = get_metabolomics_sets(
        client=client, minimum_belief=0.5, minimum_evidence_count=3
    )
    results = metabolomics_ora(client=client, chebi_ids=EXAMPLE_CHEBI_IDS)
    print(results.head())


if __name__ == "__main__":
    main()
