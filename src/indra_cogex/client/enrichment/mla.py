"""Metabolomic set analysis utilities."""

import itertools as itt
import json
from collections import defaultdict
from functools import lru_cache
from textwrap import dedent
from typing import Dict, Iterable, List, Mapping, Optional, Set, Tuple

import indra.statements
import pandas as pd
from indra.databases.hgnc_client import enzyme_to_hgncs, hgnc_to_enzymes
from indra.ontology.bio import bio_ontology
from indra.statements import stmts_from_json

from indra_cogex.client.enrichment.discrete import _do_ora
from indra_cogex.client.enrichment.utils import (
    minimum_belief_helper,
    minimum_evidence_helper,
)
from indra_cogex.client.neo4j_client import Neo4jClient

__all__ = [
    "get_metabolomics_sets",
    "EXAMPLE_CHEBI_IDS",
    "EXAMPLE_CHEBI_CURIES",
    "metabolomics_ora",
    "metabolomics_explanation",
    "get_metabolomics_network",
]


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
    # Create local helper function instead of importing this from the sources/ec module
    # to avoid importing the whole ec module
    def _strip_ec_code(raw_code: str) -> str:
        """Strips off trailing dashes from ec codes"""
        # Continue to strip off '.-' until name does not end with '.-'
        while raw_code.endswith(".-"):
            raw_code = raw_code[:-2]
        return raw_code

    rv = defaultdict(set)

    evidence_line = minimum_evidence_helper(minimum_evidence_count)
    belief_line = minimum_belief_helper(minimum_belief)
    query = dedent(
        f"""\
    MATCH
        (enzyme:BioEntity)-[:xref]-(family:BioEntity)
        -[r:indra_rel]->(chemical:BioEntity)
    WHERE
        enzyme.id STARTS WITH "ec-code"
        and family.id STARTS WITH "fplx"
        and chemical.id STARTS WITH "chebi"
        {evidence_line}
        {belief_line}
    RETURN
        enzyme.id, enzyme.name, collect(chemical.id)
    UNION ALL
    MATCH
        (enzyme:BioEntity)-[:xref]-(family:BioEntity)
        <-[:isa|partof*1..]-(gene:BioEntity)
        -[r:indra_rel]->(chemical:BioEntity)
    WHERE
        enzyme.id STARTS WITH "ec-code"
        and family.id STARTS WITH "fplx"
        and chemical.id STARTS WITH "chebi"
        {evidence_line}
        {belief_line}
    RETURN
        enzyme.id, enzyme.name, collect(chemical.id)
    """
    )
    for ec_curie, ec_name, chebi_curies in client.query_tx(query):
        ec_code = ec_curie.split(":", 1)[1]
        # There are a few cases where the name is not in the database, try to get it
        # from the bio_ontology in those cases
        if not ec_name:
            name = bio_ontology.get_name("ECCODE", ec_code)
        else:
            name = ec_name
        rv[ec_code, name].update(
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
        hgnc_id = hgnc_curie.replace("hgnc:", "", 1)
        chebi_ids = {chebi_curie.split(":", 1)[1] for chebi_curie in chebi_curies}
        for raw_ec_code in hgnc_to_enzymes.get(hgnc_id, []):
            ec_code = _strip_ec_code(raw_ec_code)
            ec_name = bio_ontology.get_name("ECCODE", ec_code)
            rv[ec_code, ec_name].update(chebi_ids)
    rv = dict(rv)
    print(f"got {len(rv)} enzymes to {_sum_values(rv)} chemicals")
    return rv


def _sum_values(d):
    return len(set(itt.chain.from_iterable(d.values())))


def metabolomics_ora(
    *,
    client: Neo4jClient,
    chebi_ids: Iterable[str],
    minimum_evidence_count: Optional[float] = None,
    minimum_belief: Optional[float] = None,
    **kwargs,
) -> pd.DataFrame:
    """Calculate over-representation on all metabolites."""
    curie_to_target_sets = get_metabolomics_sets(
        client=client,
        minimum_evidence_count=minimum_evidence_count,
        minimum_belief=minimum_belief,
    )
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
    """Get explanations for a given enzyme and metabolites query.

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
        (enzyme:BioEntity)-[:xref]-(family:BioEntity)
        -[r:indra_rel]->(chemical:BioEntity)
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
        (enzyme:BioEntity)-[:xref]-(family:BioEntity)
        <-[:isa|partof*1..]-(gene:BioEntity)
        -[r:indra_rel]->(chemical:BioEntity)
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
    rows = list(client.query_tx(query))

    # Genes mapped to EC codes outside the FamPlex hierarchy
    hgnc_ids = enzyme_to_hgncs.get(ec_code, set())
    if hgnc_ids:
        gene_query = dedent(
            f"""\
        MATCH
            (gene:BioEntity)-[r:indra_rel]->(chemical:BioEntity)
        WHERE
            gene.id IN $hgnc_curies
            and chemical.id {entity_line}
            {evidence_line}
            {belief_line}
        RETURN
            r.stmt_json
        """
        )
        rows += client.query_tx(
            gene_query,
            hgnc_curies=[f"hgnc:{hgnc_id}" for hgnc_id in hgnc_ids],
        )

    # Cross-references are stored in both directions, so rows can repeat
    seen = set()
    stmts_json = []
    for row in rows:
        if row[0] in seen:
            continue
        seen.add(row[0])
        stmts_json.append(json.loads(row[0]))
    return stmts_from_json(stmts_json)


# Node colors and shapes
METABOLITE_STYLE = ("#FF9800", "diamond")
ENZYME_STYLE = ("#4CAF50", "box")
FAMILY_STYLE = ("#1B5E20", "hexagon")

# Enrichment shading for EC class nodes, weakest to strongest
SIGNIFICANCE_COLORS = [
    (2.0, "#A5D6A7"),
    (3.0, "#66BB6A"),
    (5.0, "#43A047"),
    (10.0, "#2E7D32"),
]
STRONGEST_COLOR = "#1B5E20"
UNSCORED_COLOR = "#9E9E9E"


def significance_style(score):
    """Color and size for an EC node from its -log10 q value."""
    if score is None:
        return UNSCORED_COLOR, 22
    color = STRONGEST_COLOR
    for threshold, value in SIGNIFICANCE_COLORS:
        if score < threshold:
            color = value
            break
    return color, 18 + min(score, 12) * 2

# Edge colors per statement type
STMT_TYPE_COLORS = {
    "Activation": "#00CC00",
    "Inhibition": "#FF0000",
    "Phosphorylation": "#000000",
    "Complex": "#0000FF",
    "IncreaseAmount": "#00CC00",
    "DecreaseAmount": "#FF0000",
}
DASHED_STMT_TYPES = {"IncreaseAmount", "DecreaseAmount"}


def get_metabolomics_network(
    *,
    client: Neo4jClient,
    chebi_ids: Iterable[str],
    ec_codes: Iterable[str],
    minimum_evidence_count: Optional[float] = None,
    minimum_belief: Optional[float] = None,
    ec_significance: Optional[Mapping[str, float]] = None,
) -> Mapping[str, List[Dict]]:
    """Build a three-layer network of metabolites, enzymes and enzyme families.

    The network is laid out in three levels: the queried metabolites at level 0,
    the enzymes that regulate them at level 1, and the EC classes those enzymes
    belong to at level 2. Nodes and edges are returned in the shape expected by
    vis.js.

    Parameters
    ----------
    client :
        The Neo4j client.
    chebi_ids :
        The ChEBI local identifiers that were queried.
    ec_codes :
        The EC codes to include, in the form ``W.X.Y.Z``.
    minimum_evidence_count :
        The minimum number of evidences for a relationship to be included.
    minimum_belief :
        The minimum belief for a relationship to be included.

    Returns
    -------
    :
        A dictionary with ``nodes`` and ``edges`` keys.
    """
    chebi_curies = [f"chebi:{chebi_id}" for chebi_id in chebi_ids]
    ec_curies = [f"ec-code:{ec_code}" for ec_code in ec_codes]
    if not chebi_curies or not ec_curies:
        return {"nodes": [], "edges": []}

    evidence_line = minimum_evidence_helper(minimum_evidence_count)
    belief_line = minimum_belief_helper(minimum_belief)

    # Relationships attributed to the family, then to its member genes
    query = dedent(
        f"""\
    MATCH
        (enzyme:BioEntity)-[:xref]-(family:BioEntity)
        -[r:indra_rel]->(chemical:BioEntity)
    WHERE
        enzyme.id IN $ec_curies
        and family.id STARTS WITH "fplx"
        and chemical.id IN $chebi_curies
        {evidence_line}
        {belief_line}
    RETURN
        enzyme.id AS ec_id, enzyme.name AS ec_name,
        family.id AS mid_id, family.name AS mid_name,
        chemical.id AS met_id, chemical.name AS met_name,
        r.stmt_type AS stmt_type, r.belief AS belief,
        r.evidence_count AS evidence_count
    UNION ALL
    MATCH
        (enzyme:BioEntity)-[:xref]-(family:BioEntity)
        <-[:isa|partof*1..]-(gene:BioEntity)
        -[r:indra_rel]->(chemical:BioEntity)
    WHERE
        enzyme.id IN $ec_curies
        and family.id STARTS WITH "fplx"
        and chemical.id IN $chebi_curies
        {evidence_line}
        {belief_line}
    RETURN
        enzyme.id AS ec_id, enzyme.name AS ec_name,
        gene.id AS mid_id, gene.name AS mid_name,
        chemical.id AS met_id, chemical.name AS met_name,
        r.stmt_type AS stmt_type, r.belief AS belief,
        r.evidence_count AS evidence_count
    """
    )
    rows = list(
        client.query_tx(query, ec_curies=ec_curies, chebi_curies=chebi_curies)
    )

    # Genes mapped to EC codes outside the FamPlex hierarchy
    hgnc_to_ec = defaultdict(set)
    for ec_code in ec_codes:
        for hgnc_id in enzyme_to_hgncs.get(ec_code, []):
            hgnc_to_ec[hgnc_id].add(ec_code)
    if hgnc_to_ec:
        gene_query = dedent(
            f"""\
        MATCH
            (gene:BioEntity)-[r:indra_rel]->(chemical:BioEntity)
        WHERE
            gene.id IN $hgnc_curies
            and chemical.id IN $chebi_curies
            {evidence_line}
            {belief_line}
        RETURN
            gene.id AS mid_id, gene.name AS mid_name,
            chemical.id AS met_id, chemical.name AS met_name,
            r.stmt_type AS stmt_type, r.belief AS belief,
            r.evidence_count AS evidence_count
        """
        )
        gene_rows = client.query_tx(
            gene_query,
            hgnc_curies=[f"hgnc:{hgnc_id}" for hgnc_id in hgnc_to_ec],
            chebi_curies=chebi_curies,
        )
        for mid_id, mid_name, met_id, met_name, stmt_type, belief, \
                ev in gene_rows:
            hgnc_id = mid_id.split(":", 1)[1]
            for ec_code in hgnc_to_ec.get(hgnc_id, set()):
                ec_name = bio_ontology.get_name("ECCODE", ec_code)
                rows.append((f"ec-code:{ec_code}", ec_name, mid_id,
                             mid_name, met_id, met_name, stmt_type,
                             belief, ev))

    return assemble_metabolomics_network(rows, ec_significance)


def assemble_metabolomics_network(
    rows, ec_significance: Optional[Mapping[str, float]] = None,
) -> Mapping[str, List[Dict]]:
    """Turn (family, enzyme, metabolite) rows into vis.js nodes and edges."""
    nodes = {}
    statements_by_edge = defaultdict(list)
    membership = set()

    def add_node(node_id, label, level, style, node_type, size=None):
        if node_id in nodes:
            return
        color, shape = style
        nodes[node_id] = {
            "id": node_id,
            "label": label or node_id,
            "level": level,
            "color": color,
            "shape": shape,
            "type": node_type,
            "title": f"{node_type}: {label or node_id}",
            "details": {"id": node_id},
        }
        if size is not None:
            nodes[node_id]["size"] = size

    # Cross-references are stored in both directions, so rows can repeat
    for row in {tuple(row) for row in rows}:
        ec_id, ec_name, mid_id, mid_name, met_id, met_name, stmt_type, \
            belief, evidence_count = row
        node_type = "FPLX" if mid_id.startswith("fplx") else "HGNC"
        add_node(met_id, met_name, 2, METABOLITE_STYLE, "CHEBI")
        add_node(mid_id, mid_name, 1, ENZYME_STYLE, node_type)
        ec_code = ec_id.split(":", 1)[-1]
        score = (ec_significance or {}).get(ec_code)
        ec_color, ec_size = significance_style(score)
        add_node(ec_id, ec_name or ec_id, 0,
                 (ec_color, FAMILY_STYLE[1]), "ECCODE", ec_size)

        statements_by_edge[mid_id, met_id].append(
            {
                "statement_type": stmt_type,
                "belief": belief,
                "evidence_count": evidence_count or 0,
            }
        )
        membership.add((ec_id, mid_id))

    edges = []
    for (mid_id, met_id), statements in statements_by_edge.items():
        # The statement type with the most evidence determines the style
        statements.sort(key=lambda s: s["evidence_count"], reverse=True)
        dominant = statements[0]["statement_type"]
        total_evidence = sum(s["evidence_count"] for s in statements)
        edges.append(
            {
                "from": mid_id,
                "to": met_id,
                "color": {"color": STMT_TYPE_COLORS.get(dominant, "#999999")},
                "dashes": dominant in DASHED_STMT_TYPES,
                "arrows": {"to": {"enabled": True, "scaleFactor": 0.5}},
                "details": {
                    "statement_type": dominant,
                    "belief": statements[0]["belief"],
                    "evidence_count": total_evidence,
                    "aggregated_statements": statements,
                },
            }
        )

    for ec_id, mid_id in membership:
        edges.append(
            {
                "from": ec_id,
                "to": mid_id,
                "color": {"color": "#BBBBBB"},
                "dashes": True,
                "arrows": {"to": {"enabled": False}},
                "details": {"statement_type": "member of EC class"},
            }
        )

    return {"nodes": list(nodes.values()), "edges": edges}


# Monoamine neurotransmitters with their precursors and metabolites
EXAMPLE_CHEBI_IDS = [
    "17895",  # L-tyrosine
    "15765",  # L-dopa
    "18243",  # dopamine
    "33569",  # noradrenaline
    "33568",  # adrenaline
    "16828",  # L-tryptophan
    "28790",  # serotonin
    "16796",  # melatonin
    "18295",  # histamine
    "16865",  # gamma-aminobutyric acid
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
