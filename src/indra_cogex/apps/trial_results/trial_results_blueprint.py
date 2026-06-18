"""Blueprint for the clinical trial results browser."""

import gilda
import flask
from flask import render_template, request, redirect, url_for
from indra.ontology.bio import bio_ontology

from indra_cogex.apps.proxies import client
from indra_cogex.client.queries import (
    get_full_trial_result,
    get_metrics_for_arm,
    get_metrics_for_statistical_comparison,
    get_drugs_for_trial,
    get_diseases_for_trial,
)

_ENTITY_NAMESPACES = ["CHEBI", "MESH", "DOID", "EFO"]

__all__ = ["trial_results_blueprint"]


def _parse_entities(raw):
    """Split Neo4j BioEntity id (e.g. 'mesh:D001943') into ns/id for bioregistry badge rendering."""
    out = []
    for e in (raw or []):
        if not e or not e.get("id"):
            continue
        ns_part, _, id_part = e["id"].partition(":")
        out.append({"name": e.get("name") or e["id"], "ns": ns_part, "id": id_part})
    return out

_TOTAL_PAPERS = 13712
_processed_papers = None


def _get_processed_count() -> int:
    """Return the number of TrialResult nodes in the graph, cached after first query."""
    global _processed_papers
    if _processed_papers is None:
        rows = client.query_tx("MATCH (r:TrialResult) RETURN count(r) AS n")
        _processed_papers = rows[0][0] if rows else 0
    return _processed_papers


trial_results_blueprint = flask.Blueprint(
    "trial_results", __name__, url_prefix="/trial-results"
)


def _gene_search(ns: str, gid: str, label: str):
    """Run a trial result search via a gene (HGNC)."""
    rows = client.query_tx(
        """\
        MATCH (p:Publication)-[:has_trial_result]->(r:TrialResult)
              -[:has_genetic_criterion]->(g:BioEntity {id: $gene_id})
        OPTIONAL MATCH (ct:ClinicalTrial)-[:has_publication]->(p)
        OPTIONAL MATCH (drug:BioEntity)-[:tested_in {mesh: true}]->(ct)
        OPTIONAL MATCH (disease:BioEntity)-[:has_trial {mesh: true}]->(ct)
        RETURN r, p.id AS pub_id, max(ct.phase) AS ct_phase,
               collect(DISTINCT ct.id) AS ct_ids,
               collect(DISTINCT {name: drug.name, id: drug.id}) AS interventions,
               collect(DISTINCT {name: disease.name, id: disease.id}) AS conditions
        """,
        gene_id=f"{ns.lower()}:{gid}",
    )
    results = [
        {
            "result": client.neo4j_to_node(row[0]),
            "pmid": row[1].split(":")[-1],
            "ct_phase": row[2] if (row[2] is not None and row[2] != -1) else None,
            "ct_ids": [cid.split(":")[-1] for cid in (row[3] or [])],
            "interventions": _parse_entities(row[4]),
            "conditions": _parse_entities(row[5]),
        }
        for row in (rows or [])
    ]
    return results, label


def _run_entity_search(entity_id: str, label: str):
    """Run a trial result search via a BioEntity, trying both tested_in and has_trial."""
    rows = client.query_tx(
        """\
        MATCH (e:BioEntity {id: $entity_id})-[:tested_in|has_trial]->(ct:ClinicalTrial)
            -[:has_publication]->(p:Publication)-[:has_trial_result]->(r:TrialResult)
        OPTIONAL MATCH (drug:BioEntity)-[:tested_in {mesh: true}]->(ct)
        OPTIONAL MATCH (disease:BioEntity)-[:has_trial {mesh: true}]->(ct)
        RETURN r, p.id AS pub_id, max(ct.phase) AS ct_phase,
               collect(DISTINCT ct.id) AS ct_ids,
               collect(DISTINCT {name: drug.name, id: drug.id}) AS interventions,
               collect(DISTINCT {name: disease.name, id: disease.id}) AS conditions
        """,
        entity_id=entity_id,
    )
    results = [
        {
            "result": client.neo4j_to_node(row[0]),
            "pmid": row[1].split(":")[-1],
            "ct_phase": row[2] if (row[2] is not None and row[2] != -1) else None,
            "ct_ids": [cid.split(":")[-1] for cid in (row[3] or [])],
            "interventions": _parse_entities(row[4]),
            "conditions": _parse_entities(row[5]),
        }
        for row in (rows or [])
    ]
    return results, label


@trial_results_blueprint.route("/", methods=["GET", "POST"])
def search():
    """Search for trial results by PMID, gene, drug, or disease."""
    if request.method == "POST":
        query = request.form.get("query", "").strip()
        if not query:
            return render_template(
                "trial_results/search.html",
                error="Please enter a search term.",
                processed=_get_processed_count(),
                total=_TOTAL_PAPERS,
            )

        if query.isdigit():
            return redirect(url_for("trial_results.result", pmid=query))

        ns, _, gid = query.partition(":")

        if gid:
            ns_upper = ns.upper()
            if ns_upper == "HGNC":
                name = bio_ontology.get_name("HGNC", gid)
                label = f"{name} ({query})" if name else query
                search_results, search_label = _gene_search(ns, gid, label)
            else:
                name = bio_ontology.get_name(ns_upper, gid)
                label = f"{name} ({query})" if name else query
                search_results, search_label = _run_entity_search(
                    f"{ns.lower()}:{gid}", label
                )
        else:
            hgnc_matches = gilda.ground(query, namespaces=["HGNC"])
            if hgnc_matches:
                gid = hgnc_matches[0].term.id
                name = bio_ontology.get_name("HGNC", gid)
                label = f"{name} (hgnc:{gid})" if name else f"hgnc:{gid}"
                search_results, search_label = _gene_search("hgnc", gid, label)
            else:
                entity_matches = gilda.ground(query, namespaces=_ENTITY_NAMESPACES)
                if entity_matches:
                    m = entity_matches[0].term
                    label = f"{m.entry_name} ({m.db.lower()}:{m.id})"
                    search_results, search_label = _run_entity_search(
                        f"{m.db.lower()}:{m.id}", label
                    )
                else:
                    return render_template(
                        "trial_results/search.html",
                        error=(
                            f"Could not resolve '{query}'. Try a gene symbol "
                            "(e.g. BRCA1), drug name (e.g. trastuzumab), "
                            "or disease (e.g. breast cancer)."
                        ),
                        processed=_get_processed_count(),
                        total=_TOTAL_PAPERS,
                    )

        return render_template(
            "trial_results/search.html",
            gene_query=search_label,
            gene_results=search_results,
            processed=_get_processed_count(),
            total=_TOTAL_PAPERS,
        )
    error_pmid = request.args.get("error")
    return render_template(
        "trial_results/search.html",
        error=f"No trial result found for PMID {error_pmid}." if error_pmid else None,
        processed=_get_processed_count(),
        total=_TOTAL_PAPERS,
    )


@trial_results_blueprint.route("/result/<pmid>")
def result(pmid):
    """Display the full trial result for a given PMID."""
    data = get_full_trial_result(pmid, client=client)
    if not data:
        return redirect(url_for("trial_results.search", error=pmid))

    result_node = data["result"]
    trial_result_tuple = (result_node.db_ns, result_node.db_id)

    arms_data = []
    for arm in data["arms"]:
        arm_tuple = (arm.db_ns, arm.db_id)
        arm_id = f"{arm.db_ns.lower()}:{arm.db_id}"
        arm_metrics = list(
            get_metrics_for_arm(trial_result_tuple, arm=arm_tuple, client=client)
        )
        arm_aes = list(client.query_nodes(
            "MATCH (a:TrialArm {id: $arm_id})"
            "-[:has_adverse_event]->(ae:TrialAdverseEvent) RETURN ae",
            arm_id=arm_id,
        ))
        arms_data.append({
            "arm": arm,
            "metrics": arm_metrics,
            "adverse_events": arm_aes,
        })

    comparisons_data = []
    for sc in data["comparisons"]:
        sc_tuple = (sc.db_ns, sc.db_id)
        sc_metrics = list(
            get_metrics_for_statistical_comparison(sc_tuple, client=client)
        )
        comparisons_data.append({"comparison": sc, "metrics": sc_metrics})

    inclusion = [
        c for c in data["criteria"]
        if c.data.get("criterion_type") == "inclusion"
    ]
    exclusion = [
        c for c in data["criteria"]
        if c.data.get("criterion_type") == "exclusion"
    ]

    ct_rows = client.query_tx(
        "MATCH (ct:ClinicalTrial)-[:has_publication]->(pub:Publication {id: $pub_id}) RETURN ct.id",
        pub_id=f"pubmed:{pmid}",
    )
    ct_ids = [row[0].split(":")[-1] for row in (ct_rows or [])]

    seen_drugs, seen_diseases = {}, {}
    for nct_id in ct_ids:
        trial_tuple = ("clinicaltrials", nct_id)
        for node in get_drugs_for_trial(trial_tuple, client=client):
            key = f"{node.db_ns}:{node.db_id}"
            seen_drugs[key] = node
        for node in get_diseases_for_trial(trial_tuple, client=client):
            key = f"{node.db_ns}:{node.db_id}"
            seen_diseases[key] = node

    return render_template(
        "trial_results/result.html",
        pmid=pmid,
        data=data,
        arms_data=arms_data,
        comparisons_data=comparisons_data,
        inclusion=inclusion,
        exclusion=exclusion,
        ct_ids=ct_ids,
        drugs=list(seen_drugs.values()),
        diseases=list(seen_diseases.values()),
    )
