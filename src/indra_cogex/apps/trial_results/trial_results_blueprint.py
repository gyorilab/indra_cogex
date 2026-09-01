"""Blueprint for the clinical trial results browser."""

import gilda
import flask
from flask import render_template, request, redirect, url_for
from indra.ontology.bio import bio_ontology

from indra_cogex.apps.proxies import client
from indra_cogex.representation import norm_id
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
    seen = set()
    for e in (raw or []):
        if not e or not e.get("id"):
            continue
        ns_part, _, id_part = e["id"].partition(":")
        key = (ns_part, id_part)
        if key in seen:
            continue
        seen.add(key)
        out.append({"name": e.get("name") or e["id"], "ns": ns_part, "id": id_part})
    return out


def _is_priority(source, ref_type):
    return source == "pubmed" or (ref_type or "").upper() == "RESULT"


def _trial_tags(source_data_list):
    tags = []
    for e in source_data_list:
        if e["source"] == "pubmed":
            tag = "pubmed"
        elif e["source"] == "ctgov" and e["ref_type"]:
            tag = e["ref_type"].upper()
        else:
            continue
        if tag not in tags:
            tags.append(tag)
    return tags


def _year_label(year, anticipated):
    if year is None:
        return None
    label = str(int(year)) if isinstance(year, float) else str(year)
    if anticipated:
        label += " (anticipated)"
    return label


def _nct_tooltip(source_data_list, meta):
    lines = [
        f"Source: {e['source']}; Type: {e['ref_type']}" if e["ref_type"]
        else f"Source: {e['source']}"
        for e in source_data_list
    ]
    phase = meta.get("phase")
    if phase is not None and phase != -1:
        lines.append(f"Phase: {phase}")
    study_type = meta.get("study_type")
    if study_type:
        lines.append(f"Type: {study_type}")
    status = meta.get("status")
    if status:
        lines.append(f"Status: {status}")
    start = _year_label(meta.get("start_year"), meta.get("start_year_anticipated"))
    if start:
        lines.append(f"Start: {start}")
    completion = _year_label(
        meta.get("completion_year"), meta.get("completion_year_anticipated")
    )
    if completion:
        lines.append(f"Completion: {completion}")
    randomized = meta.get("randomized")
    if randomized is not None:
        lines.append(f"Randomized: {'yes' if randomized else 'no'}")
    why_stopped = meta.get("why_stopped")
    if why_stopped:
        lines.append(f"Why stopped: {why_stopped}")
    return "\n".join(lines)


def _group_ct_ids(ct_rows):
    grouped = {}
    order = []
    for row in ct_rows or []:
        vals = list(row) + [None] * 12
        (
            nct_id, source, ref_type, phase, status, study_type, start_year,
            start_year_anticipated, completion_year, completion_year_anticipated,
            randomized, why_stopped,
        ) = vals[:12]
        nct = nct_id.split(":")[-1]
        if nct not in grouped:
            grouped[nct] = {
                "source_data_list": [],
                "meta": {
                    "phase": phase,
                    "status": status,
                    "study_type": study_type,
                    "start_year": start_year,
                    "start_year_anticipated": start_year_anticipated,
                    "completion_year": completion_year,
                    "completion_year_anticipated": completion_year_anticipated,
                    "randomized": randomized,
                    "why_stopped": why_stopped,
                },
            }
            order.append(nct)
        source_data = {"source": source, "ref_type": ref_type}
        if source_data not in grouped[nct]["source_data_list"]:
            grouped[nct]["source_data_list"].append(source_data)

    result_ncts = []
    other_ncts = []
    for nct in order:
        entry = grouped[nct]
        source_data_list = entry["source_data_list"]
        item = {
            "nct": nct,
            "source_data_list": source_data_list,
            "tooltip": _nct_tooltip(source_data_list, entry["meta"]),
        }
        if any(_is_priority(e["source"], e["ref_type"]) for e in source_data_list):
            result_ncts.append(item)
        else:
            other_ncts.append(item)
    return result_ncts, other_ncts


def _group_search_trials(trial_rows):
    grouped = {}
    order = []
    for row in trial_rows or []:
        if not row:
            continue
        if not isinstance(row, dict):
            try:
                row = dict(row)
            except (TypeError, ValueError):
                continue
        nct_id = row.get("nct")
        if not nct_id:
            continue
        nct = nct_id.split(":")[-1]
        if nct not in grouped:
            grouped[nct] = {
                "nct": nct,
                "phase": None,
                "source_data_list": [],
                "interventions_raw": [],
                "conditions_raw": [],
            }
            order.append(nct)
        item = grouped[nct]
        source_data = {"source": row.get("source"), "ref_type": row.get("ref_type")}
        if source_data not in item["source_data_list"]:
            item["source_data_list"].append(source_data)
        phase = row.get("phase")
        if phase is not None and phase != -1:
            if item["phase"] is None or phase > item["phase"]:
                item["phase"] = phase
        item["interventions_raw"].extend(row.get("interventions") or [])
        item["conditions_raw"].extend(row.get("conditions") or [])

    trials = []
    for nct in order:
        item = grouped[nct]
        priority = any(
            _is_priority(e["source"], e["ref_type"])
            for e in item["source_data_list"]
        )
        trials.append({
            "nct": nct,
            "phase": item["phase"],
            "source_data_list": item["source_data_list"],
            "tags": _trial_tags(item["source_data_list"]),
            "priority": priority,
            "interventions": (
                _parse_entities(item["interventions_raw"]) if priority else []
            ),
            "conditions": (
                _parse_entities(item["conditions_raw"]) if priority else []
            ),
        })
    trials.sort(key=lambda t: (not t["priority"],))

    phases = [
        t["phase"] for t in trials
        if t["priority"] and t["phase"] is not None and t["phase"] != -1
    ]
    ct_phase = max(phases) if phases else None
    return trials, ct_phase


def _format_search_results(rows):
    results = []
    for row in (rows or []):
        trials, ct_phase = _group_search_trials(row[2])
        results.append({
            "result": client.neo4j_to_node(row[0]),
            "pmid": row[1].split(":")[-1],
            "ct_phase": ct_phase,
            "trials": trials,
        })
    return results


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


_TRIAL_ROW_TAIL = """\
        OPTIONAL MATCH (ct)-[:has_intervention {ctgov: true}]->(drug:BioEntity)
        OPTIONAL MATCH (ct)-[:has_condition {ctgov: true}]->(disease:BioEntity)
        WITH r, p, ct, hp,
             [x IN collect(DISTINCT {name: drug.name, id: drug.id}) WHERE x.id IS NOT NULL] AS drugs,
             [x IN collect(DISTINCT {name: disease.name, id: disease.id}) WHERE x.id IS NOT NULL] AS diseases
        RETURN r, p.id AS pub_id,
               collect({
                 nct: ct.id, phase: ct.phase, source: hp.source, ref_type: hp.ref_type,
                 interventions: drugs, conditions: diseases
               }) AS trial_rows
        """


def _gene_search(ns: str, gid: str, label: str):
    """Run a trial result search via a gene (HGNC)."""
    rows = client.query_tx("""\
        MATCH (p:Publication)-[:has_trial_result]->(r:TrialResult)
        MATCH (r:TrialResult)-[:has_genetic_criterion]->(g:BioEntity {id: $gene_id})
        OPTIONAL MATCH (ct:ClinicalTrial)-[hp:has_publication]->(p)
        WHERE ct IS NULL OR p.year IS NULL OR ct.start_year IS NULL
              OR ct.start_year <= p.year
        """ + _TRIAL_ROW_TAIL,
        gene_id=f"{ns.lower()}:{gid}",
    )
    return _format_search_results(rows), label


def _run_entity_search(entity_id: str, label: str):
    """Run a trial result search via both conditions and interventions"""
    rows = client.query_tx("""\
        MATCH (ct:ClinicalTrial)-[:has_condition|has_intervention]->(e:BioEntity {id: $entity_id})
        MATCH (ct)-[hp:has_publication]->(p:Publication)
        MATCH (p)-[:has_trial_result]->(r:TrialResult)
        WHERE p.year IS NULL OR ct.start_year IS NULL OR ct.start_year <= p.year
        """ + _TRIAL_ROW_TAIL,
        entity_id=entity_id,
    )
    return _format_search_results(rows), label


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
                entity_id = norm_id(ns, gid)
                label = f"{name} ({entity_id})" if name else entity_id
                search_results, search_label = _run_entity_search(
                    entity_id, label
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
                    entity_id = norm_id(m.db, m.id)
                    label = f"{m.entry_name} ({entity_id})"
                    search_results, search_label = _run_entity_search(
                        entity_id, label
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
                    )

        return render_template(
            "trial_results/search.html",
            gene_query=search_label,
            gene_results=search_results,
            processed=_get_processed_count(),
        )
    error_pmid = request.args.get("error")
    return render_template(
        "trial_results/search.html",
        error=f"No trial result found for PMID {error_pmid}." if error_pmid else None,
        processed=_get_processed_count(),
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
        """\
        MATCH (ct:ClinicalTrial)-[r:has_publication]->(pub:Publication {id: $pub_id})
        WHERE toUpper(coalesce(r.ref_type, '')) <> 'RESULT'
              OR pub.year IS NULL OR ct.start_year IS NULL
              OR ct.start_year <= pub.year
        RETURN ct.id, r.source, r.ref_type,
               ct.phase, ct.status, ct.study_type, ct.start_year,
               ct.start_year_anticipated, ct.completion_year,
               ct.completion_year_anticipated, ct.randomized, ct.why_stopped""",
        pub_id=f"pubmed:{pmid}",
    )
    result_ncts, other_ncts = _group_ct_ids(ct_rows)

    trial_entities = []
    for item in result_ncts:
        trial_tuple = ("clinicaltrials", item["nct"])
        trial_entities.append({
            "nct_item": item,
            "drugs": list(get_drugs_for_trial(trial_tuple, client=client)),
            "diseases": list(get_diseases_for_trial(trial_tuple, client=client)),
        })

    return render_template(
        "trial_results/result.html",
        pmid=pmid,
        data=data,
        arms_data=arms_data,
        comparisons_data=comparisons_data,
        inclusion=inclusion,
        exclusion=exclusion,
        result_ncts=result_ncts,
        other_ncts=other_ncts,
        trial_entities=trial_entities,
    )
