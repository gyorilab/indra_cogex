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
)

__all__ = ["trial_results_blueprint"]

trial_results_blueprint = flask.Blueprint(
    "trial_results", __name__, url_prefix="/trial-results"
)


@trial_results_blueprint.route("/", methods=["GET", "POST"])
def search():
    """Search for trial results by PMID or gene."""
    if request.method == "POST":
        search_type = request.form.get("search_type", "pmid")
        query = request.form.get("query", "").strip()
        if not query:
            return render_template(
                "trial_results/search.html",
                error="Please enter a search term.",
            )
        if search_type == "pmid" and not query.isdigit():
            search_type = "gene"
        if search_type == "gene" and query.isdigit():
            search_type = "pmid"
        if search_type == "pmid":
            return redirect(url_for("trial_results.result", pmid=query))
        else:
            ns, _, gid = query.partition(":")
            if not gid:
                matches = gilda.ground(query, namespaces=["HGNC"])
                if not matches:
                    return render_template(
                        "trial_results/search.html",
                        error=(
                            f"Could not resolve gene '{query}'. "
                            "Try a symbol (e.g. BRCA1) or HGNC ID (e.g. hgnc:1100)."
                        ),
                    )
                ns = "hgnc"
                gid = matches[0].term.id
                query = f"hgnc:{gid}"
            gene_name = bio_ontology.get_name("HGNC", gid)
            gene_label = f"{gene_name} ({query})" if gene_name else query
            rows = client.query_tx(
                "MATCH (p:Publication)-[:has_trial_result]->(r:TrialResult)"
                "-[:has_genetic_criterion]->(g:BioEntity {id: $gene_id}) "
                "RETURN r, p.id AS pub_id",
                gene_id=f"{ns.lower()}:{gid}",
            )
            gene_results = [
                {
                    "result": client.neo4j_to_node(row[0]),
                    "pmid": row[1].split(":")[-1],
                }
                for row in (rows or [])
            ]
            return render_template(
                "trial_results/search.html",
                gene_query=gene_label,
                gene_results=gene_results,
            )
    return render_template("trial_results/search.html")


@trial_results_blueprint.route("/result/<pmid>")
def result(pmid):
    """Display the full trial result for a given PMID."""
    data = get_full_trial_result(pmid, client=client)
    if not data:
        flask.abort(404, description=f"No trial result found for PMID {pmid}.")

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

    return render_template(
        "trial_results/result.html",
        pmid=pmid,
        data=data,
        arms_data=arms_data,
        comparisons_data=comparisons_data,
        inclusion=inclusion,
        exclusion=exclusion,
    )
