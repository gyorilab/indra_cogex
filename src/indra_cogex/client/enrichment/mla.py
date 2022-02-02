"""Metabolomic set analysis utilities."""

from functools import lru_cache
from textwrap import dedent
from typing import Mapping, Optional, Set

from tabulate import tabulate

from indra_cogex.client.neo4j_client import Neo4jClient

__all__ = [
    "get_metabolomics_sets",
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
) -> Mapping[str, Set[str]]:
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
        enzyme.id, collect(chemical.id)
    LIMIT 5;
    """
    )
    return {
        ec_curie.split(":", 1)[1]: {
            chebi_curie.split(":", 1)[1] for chebi_curie in chebi_curies
        }
        for ec_curie, chebi_curies in client.query_tx(query)
    }


def main():
    client = Neo4jClient()
    results = get_metabolomics_sets(
        client=client, minimum_belief=0.5, minimum_evidence_count=3
    )
    print(tabulate(results.items()))


if __name__ == "__main__":
    main()
