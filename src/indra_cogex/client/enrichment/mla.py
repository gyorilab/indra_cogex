"""Metabolomic set analysis utilities."""

import itertools as itt
import json
from collections import defaultdict
from functools import lru_cache
from textwrap import dedent
from typing import Iterable, List, Mapping, Optional, Set, Tuple

import indra.statements
import pandas as pd
import pyobo
import pystow
import requests
from indra.statements import stmts_from_json

from indra_cogex.client.enrichment.discrete import _do_ora
from indra_cogex.client.enrichment.utils import minimum_belief_helper, minimum_evidence_helper
from indra_cogex.client.neo4j_client import Neo4jClient

__all__ = [
    "get_metabolomics_sets",
    "EXAMPLE_CHEBI_IDS",
    "EXAMPLE_CHEBI_CURIES",
    "metabolomics_ora",
]

ENZYME_FILE = pystow.join("indra", "cogex", "ec", name="hgnc_to_ec.json")


@lru_cache(1)
def get_hgnc_to_ec() -> Mapping[str, Set[str]]:
    """Get the gene-enzyme mappings."""
    if ENZYME_FILE.is_file():
        d = json.loads(ENZYME_FILE.read_text())
    else:
        url = "https://www.genenames.org/cgi-bin/download/custom"
        params = {
            "col": ["gd_hgnc_id", "gd_enz_ids"],
            "status": "Approved",
            "hgnc_dbtag": "on",
            "order_by": "gd_app_sym_sort",
            "format": "text",
            "submit": "submit",
        }
        res = requests.get(url, params=params)
        d = {}
        lines = res.iter_lines(decode_unicode=True)
        _header = next(lines)
        for line in lines:
            try:
                hgnc_curie, enzyme_ids = line.strip().split("\t")
            except ValueError:
                continue  # no enzymes
            if not enzyme_ids:
                continue
            d[hgnc_curie.removeprefix("HGNC:")] = sorted(
                enzyme_id.strip() for enzyme_id in enzyme_ids.split(", ")
            )
        ENZYME_FILE.write_text(json.dumps(d, indent=2))
    return d


HGNC_TO_EC = get_hgnc_to_ec()


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
    rv = defaultdict(set)

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
        {evidence_line}
        {belief_line}
    RETURN
        enzyme.id, collect(chemical.id)
    UNION ALL
    MATCH
        (enzyme:BioEntity)-[:xref]-(family:BioEntity)<-[:isa|partof*1..]-(gene:BioEntity)-[r:indra_rel]->(chemical:BioEntity)
    WHERE
        enzyme.id STARTS WITH "ec-code"
        and family.id STARTS WITH "fplx"
        and chemical.id STARTS WITH "chebi"
        {evidence_line}
        {belief_line}
    RETURN
        enzyme.id, collect(chemical.id)
    """
    )
    for ec_curie, chebi_curies in client.query_tx(query):
        ec_code = ec_curie.split(":", 1)[1]
        rv[ec_code, pyobo.get_name("ec", ec_code)].update(
            {chebi_curie.split(":", 1)[1] for chebi_curie in chebi_curies}
        )

    query = dedent(
        f"""\
    MATCH
        (gene:BioEntity)-[r:indra_rel]->(chemical:BioEntity)
    WHERE
        gene.id STARTS WITH "hgnc"
        and chemical.id STARTS WITH "chebi"
        {evidence_line}
        {belief_line}
    RETURN
        gene.id, collect(chemical.id)
    """
    )
    for hgnc_curie, chebi_curies in client.query_tx(query):
        hgnc_id = hgnc_curie.removeprefix("hgnc:")
        chebi_ids = {chebi_curie.split(":", 1)[1] for chebi_curie in chebi_curies}
        for ec_code in HGNC_TO_EC.get(hgnc_id, []):
            rv[ec_code, pyobo.get_name("ec", ec_code)].update(chebi_ids)
    rv = dict(rv)
    print(f"got {len(rv)} enzymes to {_sum_values(rv)} chemicals")
    return rv


def _sum_values(d):
    return len(set(itt.chain.from_iterable(d.values())))


def metabolomics_ora(
    *, client: Neo4jClient, chebi_ids: Iterable[str], **kwargs
) -> pd.DataFrame:
    """Calculate over-representation on all metabolites."""
    curie_to_target_sets = get_metabolomics_sets(client=client)
    count = _sum_values(curie_to_target_sets)
    return _do_ora(curie_to_target_sets, query=chebi_ids, count=count, **kwargs)


def metabolomics_explanation(
    *,
    ec_code: str,
    chebi_ids: Optional[List[str]] = None,
    minimum_evidence_count: Optional[float] = None,
    minimum_belief: Optional[float] = None,
    client: Neo4jClient,
) -> List[indra.statements.Statement]:
    """

    Parameters
    ----------
    ec_code:
        The enzyme class code in the form of ``W.X.Y.Z``
    chebi_ids:
        An optional list of CURIEs to filter by
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
    A list of INDRA statements
    """
    evidence_line = minimum_evidence_helper(minimum_evidence_count)
    belief_line = minimum_belief_helper(minimum_belief)
    if chebi_ids:
        entity_line = "IN [{}]".format(
            ", ".join(f'"chebi:{chebi_id}"' for chebi_id in chebi_ids)
        )
    else:
        entity_line = 'STARTS WITH "chebi"'

    # TODO consider enzyme->entity and enzyme->gene->entity query
    query = dedent(
        f"""\
    MATCH
        (enzyme:BioEntity)-[:xref]-(family:BioEntity)-[r:indra_rel]->(chemical:BioEntity)
    WHERE
        enzyme.id IN ["ec-code:{ec_code}"]
        and family.id STARTS WITH "fplx"
        and chemical.id {entity_line}
        {evidence_line}
        {belief_line}
    RETURN
        r.stmt_json
    UNION ALL
    MATCH
        (enzyme:BioEntity)-[:xref]-(family:BioEntity)<-[:isa|partof*1..]-(gene:BioEntity)-[r:indra_rel]->(chemical:BioEntity)
    WHERE
        enzyme.id in ["ec-code:{ec_code}"]
        and family.id STARTS WITH "fplx"
        and chemical.id {entity_line}
        {evidence_line}
        {belief_line}
    RETURN
        r.stmt_json
    """
    )
    stmts_json = [json.loads(row[0]) for row in client.query_tx(query)]
    stmts = stmts_from_json(stmts_json)
    # TODO add some deduplication
    return stmts


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
    stmts = metabolomics_explanation(client=client, ec_code="1.1.1.1")
    # TODO do some grouping of statements since they all only have one evidence
    for stmt in stmts:
        print(stmt)

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
                for (ec_code, name), chebi_ids in sorted(results.items())
            )
        )
    )


if __name__ == "__main__":
    _main()
