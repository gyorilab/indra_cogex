# -*- coding: utf-8 -*-

"""An app wrapping the query module of indra_cogex."""
import logging

import flask
from flask import request, abort, Response, jsonify
from more_click import make_web_command

from indra_cogex.client.neo4j_client import Neo4jClient
from indra_cogex.client.queries import *

app = flask.Flask(__name__)

client = Neo4jClient()


logger = logging.getLogger(__name__)


@app.route("/get_genes_in_tissue", methods=["POST"])
def genes_in_tissue():
    """Get genes for a disease."""
    if request.json is None:
        abort(Response("Missing application/json header.", 415))

    disease_name = request.json.get("disease_name")
    if disease_name is None:
        abort(Response("Parameter 'disease_name' not provided", 415))
    genes = get_genes_in_tissue(client, disease_name)
    return jsonify([g.to_json() for g in genes])


@app.route("/get_tissues_for_gene", methods=["POST"])
def tissues_for_gene():
    """Get tissues for a gene."""
    if request.json is None:
        abort(Response("Missing application/json header.", 415))

    gene_name = request.json.get("gene_name")
    if gene_name is None:
        abort(Response("Parameter 'gene_name' not provided", 415))
    tissues = get_tissues_for_gene(client, gene_name)
    return jsonify([t.to_json() for t in tissues])


@app.route("/is_gene_in_tissue", methods=["POST"])
def gene_in_tissue():
    """Check if a gene is in a tissue."""
    if request.json is None:
        abort(Response("Missing application/json header.", 415))

    gene_name = request.json.get("gene_name")
    if gene_name is None:
        abort(Response("Parameter 'gene_name' not provided", 415))
    tissue_name = request.json.get("tissue_name")
    if tissue_name is None:
        abort(Response("Parameter 'tissue_name' not provided", 415))
    is_in = is_gene_in_tissue(client, gene_name, tissue_name)
    return jsonify({"gene_in_tissue": is_in})


@app.route("/get_go_terms_for_gene", methods=["POST"])
def go_terms_for_gene():
    """Get GO terms for a gene."""
    if request.json is None:
        abort(Response("Missing application/json header.", 415))

    gene_name = request.json.get("gene_name")
    if gene_name is None:
        abort(Response("Parameter 'gene_name' not provided", 415))
    go_terms = get_go_terms_for_gene(client, gene_name)
    return jsonify([t.to_json() for t in go_terms])


@app.route("/get_genes_for_go_term", methods=["POST"])
def genes_for_go_term():
    """Get genes for a GO term."""
    if request.json is None:
        abort(Response("Missing application/json header.", 415))

    go_term_name = request.json.get("go_term_name")
    if go_term_name is None:
        abort(Response("Parameter 'go_term_name' not provided", 415))
    genes = get_genes_for_go_term(client, go_term_name)
    return jsonify([g.to_json() for g in genes])


@app.route("/is_go_term_for_gene", methods=["POST"])
def go_term_for_gene():
    """Check if a GO term is for a gene."""
    if request.json is None:
        abort(Response("Missing application/json header.", 415))

    go_term_name = request.json.get("go_term_name")
    if go_term_name is None:
        abort(Response("Parameter 'go_term_name' not provided", 415))
    gene_name = request.json.get("gene_name")
    if gene_name is None:
        abort(Response("Parameter 'gene_name' not provided", 415))
    is_in = is_go_term_for_gene(client, go_term_name, gene_name)
    return jsonify({"go_term_for_gene": is_in})


@app.route("/get_trials_for_drug", methods=["POST"])
def trials_for_drug():
    """Get trials for a drug."""
    if request.json is None:
        abort(Response("Missing application/json header.", 415))

    drug_name = request.json.get("drug_name")
    if drug_name is None:
        abort(Response("Parameter 'drug_name' not provided", 415))
    trials = get_trials_for_drug(client, drug_name)
    return jsonify([t.to_json() for t in trials])


@app.route("/get_trials_for_disease", methods=["POST"])
def trials_for_disease():
    """Get trials for a disease."""
    if request.json is None:
        abort(Response("Missing application/json header.", 415))

    disease_name = request.json.get("disease_name")
    if disease_name is None:
        abort(Response("Parameter 'disease_name' not provided", 415))
    trials = get_trials_for_disease(client, disease_name)
    return jsonify([t.to_json() for t in trials])


@app.route("/get_drugs_for_trial", methods=["POST"])
def drugs_for_trial():
    """Get drugs for a trial."""
    if request.json is None:
        abort(Response("Missing application/json header.", 415))

    trial_name = request.json.get("trial_name")
    if trial_name is None:
        abort(Response("Parameter 'trial_name' not provided", 415))
    drugs = get_drugs_for_trial(client, trial_name)
    return jsonify([d.to_json() for d in drugs])


@app.route("/get_diseases_for_trial", methods=["POST"])
def diseases_for_trial():
    """Get diseases for a trial."""
    if request.json is None:
        abort(Response("Missing application/json header.", 415))

    trial_name = request.json.get("trial_name")
    if trial_name is None:
        abort(Response("Parameter 'trial_name' not provided", 415))
    diseases = get_diseases_for_trial(client, trial_name)
    return jsonify([d.to_json() for d in diseases])


@app.route("/get_pathways_for_gene", methods=["POST"])
def pathways_for_gene():
    """Get pathways for a gene."""
    if request.json is None:
        abort(Response("Missing application/json header.", 415))

    gene_name = request.json.get("gene_name")
    if gene_name is None:
        abort(Response("Parameter 'gene_name' not provided", 415))
    pathways = get_pathways_for_gene(client, gene_name)
    return jsonify([p.to_json() for p in pathways])


@app.route("/get_genes_for_pathway", methods=["POST"])
def genes_for_pathway():
    """Get genes for a pathway."""
    if request.json is None:
        abort(Response("Missing application/json header.", 415))

    pathway_name = request.json.get("pathway_name")
    if pathway_name is None:
        abort(Response("Parameter 'pathway_name' not provided", 415))
    genes = get_genes_for_pathway(client, pathway_name)
    return jsonify([g.to_json() for g in genes])


@app.route("/is_gene_in_pathway", methods=["POST"])
def gene_in_pathway():
    """Check if a gene is in a pathway."""
    if request.json is None:
        abort(Response("Missing application/json header.", 415))

    gene_name = request.json.get("gene_name")
    if gene_name is None:
        abort(Response("Parameter 'gene_name' not provided", 415))
    pathway_name = request.json.get("pathway_name")
    if pathway_name is None:
        abort(Response("Parameter 'pathway_name' not provided", 415))
    is_in = is_gene_in_pathway(client, gene_name, pathway_name)
    return jsonify({"gene_in_pathway": is_in})


@app.route("/get_side_effects_for_drug", methods=["POST"])
def side_effects_for_drug():
    """Get side effects for a drug."""
    if request.json is None:
        abort(Response("Missing application/json header.", 415))

    drug_name = request.json.get("drug_name")
    if drug_name is None:
        abort(Response("Parameter 'drug_name' not provided", 415))
    side_effects = get_side_effects_for_drug(client, drug_name)
    return jsonify([s.to_json() for s in side_effects])


@app.route("/get_drugs_for_side_effect", methods=["POST"])
def drugs_for_side_effect():
    """Get drugs for a side effect."""
    if request.json is None:
        abort(Response("Missing application/json header.", 415))

    side_effect_name = request.json.get("side_effect_name")
    if side_effect_name is None:
        abort(Response("Parameter 'side_effect_name' not provided", 415))
    drugs = get_drugs_for_side_effect(client, side_effect_name)
    return jsonify([d.to_json() for d in drugs])


@app.route("/is_side_effect_for_drug", methods=["POST"])
def side_effect_for_drug():
    """Check if a side effect is for a drug."""
    if request.json is None:
        abort(Response("Missing application/json header.", 415))

    side_effect_name = request.json.get("side_effect_name")
    if side_effect_name is None:
        abort(Response("Parameter 'side_effect_name' not provided", 415))
    drug_name = request.json.get("drug_name")
    if drug_name is None:
        abort(Response("Parameter 'drug_name' not provided", 415))
    is_in = is_side_effect_for_drug(client, side_effect_name, drug_name)
    return jsonify({"side_effect_for_drug": is_in})


@app.route("/get_ontology_child_terms", methods=["POST"])
def ontology_child_terms():
    """Get child terms of a term."""
    if request.json is None:
        abort(Response("Missing application/json header.", 415))

    term_name = request.json.get("term_name")
    if term_name is None:
        abort(Response("Parameter 'term_name' not provided", 415))
    child_terms = get_ontology_child_terms(client, term_name)
    return jsonify([t.to_json() for t in child_terms])


@app.route("/get_ontology_parent_terms", methods=["POST"])
def ontology_parent_terms():
    """Get parent terms of a term."""
    if request.json is None:
        abort(Response("Missing application/json header.", 415))

    term_name = request.json.get("term_name")
    if term_name is None:
        abort(Response("Parameter 'term_name' not provided", 415))
    parent_terms = get_ontology_parent_terms(client, term_name)
    return jsonify([t.to_json() for t in parent_terms])


@app.route("/isa_or_partof", methods=["POST"])
def isa_or_partof():
    """Check if a term is a part of another term."""
    if request.json is None:
        abort(Response("Missing application/json header.", 415))

    term_name = request.json.get("term_name")
    if term_name is None:
        abort(Response("Parameter 'term_name' not provided", 415))
    parent_term_name = request.json.get("parent_term_name")
    if parent_term_name is None:
        abort(Response("Parameter 'parent_term_name' not provided", 415))
    is_isa = isa_or_partof(client, term_name, parent_term_name)
    return jsonify({"isa_or_partof": is_isa})


@app.route("/get_pmids_for_mesh", methods=["POST"])
def pmids_for_mesh():
    """Get pmids for a mesh term."""
    if request.json is None:
        abort(Response("Missing application/json header.", 415))

    mesh_term_name = request.json.get("mesh_term_name")
    if mesh_term_name is None:
        abort(Response("Parameter 'mesh_term_name' not provided", 415))
    pmids = get_pmids_for_mesh(client, mesh_term_name)
    return jsonify([p.to_json() for p in pmids])


@app.route("/get_mesh_ids_for_pmid", methods=["POST"])
def mesh_ids_for_pmid():
    """Get mesh ids for a pmid."""
    if request.json is None:
        abort(Response("Missing application/json header.", 415))

    pmid = request.json.get("pmid")
    if pmid is None:
        abort(Response("Parameter 'pmid' not provided", 415))
    mesh_ids = get_mesh_ids_for_pmid(client, pmid)
    return jsonify([m.to_json() for m in mesh_ids])


@app.route("/get_evidences_for_mesh", methods=["POST"])
def evidences_for_mesh():
    """Get evidences for a mesh term."""
    if request.json is None:
        abort(Response("Missing application/json header.", 415))

    mesh_term_name = request.json.get("mesh_term_name")
    if mesh_term_name is None:
        abort(Response("Parameter 'mesh_term_name' not provided", 415))
    evidences = get_evidences_for_mesh(client, mesh_term_name)
    return jsonify({h: [e.to_json() for e in el] for h, el in evidences.items()})


@app.route("/get_evidences_for_stmt_hash", methods=["POST"])
def evidences_for_stmt_hash():
    """Get evidences for a statement hash."""
    if request.json is None:
        abort(Response("Missing application/json header.", 415))

    stmt_hash = request.json.get("stmt_hash")
    if stmt_hash is None:
        abort(Response("Parameter 'stmt_hash' not provided", 415))
    evidences = get_evidences_for_stmt_hash(client, stmt_hash)
    return jsonify([e.to_json() for e in evidences])


@app.route("/get_evidences_for_stmt_hashes", methods=["POST"])
def evidences_for_stmt_hashes():
    """Get evidences for a list of statement hashes."""
    if request.json is None:
        abort(Response("Missing application/json header.", 415))

    stmt_hashes = request.json.get("stmt_hashes")
    if stmt_hashes is None:
        abort(Response("Parameter 'stmt_hashes' not provided", 415))
    evidences = get_evidences_for_stmt_hashes(client, stmt_hashes)
    return jsonify({h: [e.to_json() for e in el] for h, el in evidences.items()})


@app.route("/get_stmts_for_pmid", methods=["POST"])
def stmts_for_pmid():
    """Get statements for a pmid."""
    if request.json is None:
        abort(Response("Missing application/json header.", 415))

    pmid = request.json.get("pmid")
    if pmid is None:
        abort(Response("Parameter 'pmid' not provided", 415))
    stmts = get_stmts_for_pmid(client, pmid)
    return jsonify([s.to_json() for s in stmts])


@app.route("/get_stmts_for_mesh", methods=["POST"])
def stmts_for_mesh():
    """Get statements for a mesh term."""
    if request.json is None:
        abort(Response("Missing application/json header.", 415))

    mesh_term_name = request.json.get("mesh_term_name")
    if mesh_term_name is None:
        abort(Response("Parameter 'mesh_term_name' not provided", 415))
    stmts = get_stmts_for_mesh(client, mesh_term_name)
    return jsonify([s.to_json() for s in stmts])


@app.route("/get_stmts_for_stmt_hashes", methods=["POST"])
def stmts_for_stmt_hashes():
    """Get statements for a list of statement hashes."""
    if request.json is None:
        abort(Response("Missing application/json header.", 415))

    stmt_hashes = request.json.get("stmt_hashes")
    if stmt_hashes is None:
        abort(Response("Parameter 'stmt_hashes' not provided", 415))
    stmts = get_stmts_for_stmt_hashes(client, stmt_hashes)
    return jsonify([s.to_json() for s in stmts])


cli = make_web_command(app=app)


if __name__ == "__main__":
    cli()
