# -*- coding: utf-8 -*-

"""An app wrapping the query module of indra_cogex."""
import logging
from typing import Tuple, Union

import flask
from flask import request, abort, Response, jsonify
from more_click import make_web_command

from indra_cogex.client.neo4j_client import Neo4jClient
from indra_cogex.client.queries import *

app = flask.Flask(__name__)

client = Neo4jClient()


logger = logging.getLogger(__name__)

Tup = Tuple[str, str]
TupOfTups = Tuple[Tup, ...]


def _post_request_preproc(*keys) -> Union[TupOfTups, Tup, str, int]:
    if request.json is None:
        abort(Response("Missing application/json header.", 415))
    tups = []
    for key in keys:
        if key not in request.json:
            abort(Response("Parameter '%s' not provided" % key, 415))
        tups.append(tuple(request.json[key]))

    if len(tups) == 1:
        return tups[0]

    return tuple(tups)


@app.route("/get_genes_in_tissue", methods=["POST"])
def genes_in_tissue():
    """Get genes for a disease."""
    tissue = _post_request_preproc("tissue")
    genes = get_genes_in_tissue(tissue=tissue, client=client)
    return jsonify([g.to_json() for g in genes])


@app.route("/get_tissues_for_gene", methods=["POST"])
def tissues_for_gene():
    """Get tissues for a gene."""
    gene = _post_request_preproc("gene")
    tissues = get_tissues_for_gene(gene=gene, client=client)
    return jsonify([t.to_json() for t in tissues])


@app.route("/is_gene_in_tissue", methods=["POST"])
def gene_in_tissue():
    """Check if a gene is in a tissue."""
    gene, tissue = _post_request_preproc("gene", "tissue")
    return jsonify(
        {"gene_in_tissue": is_gene_in_tissue(gene=gene, tissue=tissue, client=client)}
    )


@app.route("/get_go_terms_for_gene", methods=["POST"])
def go_terms_for_gene():
    """Get GO terms for a gene."""
    gene = _post_request_preproc("gene")
    go_terms = get_go_terms_for_gene(gene=gene, client=client)
    return jsonify([t.to_json() for t in go_terms])


@app.route("/get_genes_for_go_term", methods=["POST"])
def genes_for_go_term():
    """Get genes for a GO term."""
    go_term = _post_request_preproc("go_term")
    genes = get_genes_for_go_term(go_term=go_term, client=client)
    return jsonify([g.to_json() for g in genes])


@app.route("/is_go_term_for_gene", methods=["POST"])
def go_term_for_gene():
    """Check if a GO term is for a gene."""
    gene, go_term = _post_request_preproc("gene", "go_term")
    return jsonify(
        {
            "go_term_for_gene": is_go_term_for_gene(
                gene=gene, go_term=go_term, client=client
            )
        }
    )


@app.route("/get_trials_for_drug", methods=["POST"])
def trials_for_drug():
    """Get trials for a drug."""
    drug = _post_request_preproc("drug")
    trials = get_trials_for_drug(drug=drug, client=client)
    return jsonify([t.to_json() for t in trials])


@app.route("/get_trials_for_disease", methods=["POST"])
def trials_for_disease():
    """Get trials for a disease."""
    disease = _post_request_preproc("disease")
    trials = get_trials_for_disease(disease=disease, client=client)
    return jsonify([t.to_json() for t in trials])


@app.route("/get_drugs_for_trial", methods=["POST"])
def drugs_for_trial():
    """Get drugs for a trial."""
    trial = _post_request_preproc("trial")
    drugs = get_drugs_for_trial(trial=trial, client=client)
    return jsonify([d.to_json() for d in drugs])


@app.route("/get_diseases_for_trial", methods=["POST"])
def diseases_for_trial():
    """Get diseases for a trial."""
    trial = _post_request_preproc("trial")
    diseases = get_diseases_for_trial(trial=trial, client=client)
    return jsonify([d.to_json() for d in diseases])


@app.route("/get_pathways_for_gene", methods=["POST"])
def pathways_for_gene():
    """Get pathways for a gene."""
    gene = _post_request_preproc("gene")
    pathways = get_pathways_for_gene(gene=gene, client=client)
    return jsonify([p.to_json() for p in pathways])


@app.route("/get_shared_pathways_for_genes", methods=["POST"])
def shared_pathways_for_genes():
    """Get shared genes for a pathway."""
    genes = _post_request_preproc("genes")
    if not isinstance(genes, list):
        genes = [genes]
    pathways = get_shared_pathways_for_genes(genes=genes, client=client)
    return jsonify([p.to_json() for p in pathways])


@app.route("/get_genes_for_pathway", methods=["POST"])
def genes_for_pathway():
    """Get genes for a pathway."""
    pathway = _post_request_preproc("pathway")
    genes = get_genes_for_pathway(pathway=pathway, client=client)
    return jsonify([g.to_json() for g in genes])


@app.route("/is_gene_in_pathway", methods=["POST"])
def gene_in_pathway():
    """Check if a gene is in a pathway."""
    gene, pathway = _post_request_preproc("gene", "pathway")
    return jsonify(
        {
            "gene_in_pathway": is_gene_in_pathway(
                gene=gene, pathway=pathway, client=client
            )
        }
    )


@app.route("/get_side_effects_for_drug", methods=["POST"])
def side_effects_for_drug():
    """Get side effects for a drug."""
    drug = _post_request_preproc("drug")
    side_effects = get_side_effects_for_drug(drug=drug, client=client)
    return jsonify([s.to_json() for s in side_effects])


@app.route("/get_drugs_for_side_effect", methods=["POST"])
def drugs_for_side_effect():
    """Get drugs for a side effect."""
    side_effect = _post_request_preproc("side_effect")
    drugs = get_drugs_for_side_effect(side_effect=side_effect, client=client)
    return jsonify([d.to_json() for d in drugs])


@app.route("/is_side_effect_for_drug", methods=["POST"])
def side_effect_for_drug():
    """Check if a side effect is for a drug."""
    drug, side_effect = _post_request_preproc("drug", "side_effect")
    return jsonify(
        {
            "side_effect_for_drug": is_side_effect_for_drug(
                drug=drug, side_effect=side_effect, client=client
            )
        }
    )


@app.route("/get_ontology_child_terms", methods=["POST"])
def ontology_child_terms():
    """Get child terms of a term."""
    term = _post_request_preproc("term")
    child_terms = get_ontology_child_terms(term=term, client=client)
    return jsonify([t.to_json() for t in child_terms])


@app.route("/get_ontology_parent_terms", methods=["POST"])
def ontology_parent_terms():
    """Get parent terms of a term."""
    term = _post_request_preproc("term")
    parent_terms = get_ontology_parent_terms(term=term, client=client)
    return jsonify([t.to_json() for t in parent_terms])


@app.route("/isa_or_partof", methods=["POST"])
def isa_or_partof():
    """Check if a term is a part of another term."""
    term, parent_term = _post_request_preproc("term", "parent_term")
    return jsonify(
        {
            "isa_or_partof": isa_or_partof(
                term=term, parent_term=parent_term, client=client
            )
        }
    )


@app.route("/get_pmids_for_mesh", methods=["POST"])
def pmids_for_mesh():
    """Get pmids for a mesh term."""
    mesh_term = _post_request_preproc("mesh_term")
    pmids = get_pmids_for_mesh(mesh_term=mesh_term, client=client)
    return jsonify([p.to_json() for p in pmids])


@app.route("/get_mesh_ids_for_pmid", methods=["POST"])
def mesh_ids_for_pmid():
    """Get mesh ids for a pmid."""
    pmid_term = _post_request_preproc("pmid_term")
    mesh_ids = get_mesh_ids_for_pmid(pmid_term=pmid_term, client=client)
    return jsonify([m.to_json() for m in mesh_ids])


@app.route("/get_evidences_for_mesh", methods=["POST"])
def evidences_for_mesh():
    """Get evidences for a mesh term."""
    mesh_term = _post_request_preproc("mesh_term")
    evidence_dict = get_evidences_for_mesh(mesh_term=mesh_term, client=client)
    return jsonify({h: [e.to_json() for e in el] for h, el in evidence_dict.items()})


@app.route("/get_evidences_for_stmt_hash", methods=["POST"])
def evidences_for_stmt_hash():
    """Get evidences for a statement hash."""
    stmt_hash = _post_request_preproc("stmt_hash")
    evidences = get_evidences_for_stmt_hash(stmt_hash=stmt_hash, client=client)
    return jsonify([e.to_json() for e in evidences])


@app.route("/get_evidences_for_stmt_hashes", methods=["POST"])
def evidences_for_stmt_hashes():
    """Get evidences for a list of statement hashes."""
    stmt_hashes = _post_request_preproc("stmt_hashes")
    evidence_dict = get_evidences_for_stmt_hashes(
        stmt_hashes=stmt_hashes, client=client
    )
    return jsonify({h: [e.to_json() for e in el] for h, el in evidence_dict.items()})


@app.route("/get_stmts_for_pmid", methods=["POST"])
def stmts_for_pmid():
    """Get statements for a pmid."""
    pmid_term = _post_request_preproc("pmid_term")
    stmts = get_stmts_for_pmid(pmid_term=pmid_term, client=client)
    return jsonify([s.to_json() for s in stmts])


@app.route("/get_stmts_for_mesh", methods=["POST"])
def stmts_for_mesh():
    """Get statements for a mesh term."""
    mesh_term = _post_request_preproc("mesh_term")
    stmts = get_stmts_for_mesh(mesh_term=mesh_term, client=client)
    return jsonify([s.to_json() for s in stmts])


@app.route("/get_stmts_for_stmt_hashes", methods=["POST"])
def stmts_for_stmt_hashes():
    """Get statements for a list of statement hashes."""
    stmt_hashes = _post_request_preproc("stmt_hashes")
    stmts = get_stmts_for_stmt_hashes(stmt_hashes=stmt_hashes, client=client)
    return jsonify([s.to_json() for s in stmts])


@app.route("/is_gene_mutated", methods=["POST"])
def gene_mutated():
    """Check if a gene is mutated in a cell line"""
    gene, cell_line = _post_request_preproc("gene", "cell_line")
    return jsonify(
        {
            "is_gene_mutated": is_gene_mutated(
                gene=gene, cell_line=cell_line, client=client
            )
        }
    )


@app.route("/get_drugs_for_target", methods=["POST"])
def drugs_for_target():
    """Get drugs for a target."""
    target = _post_request_preproc("target")
    drugs = get_drugs_for_target(target=target, client=client)
    return jsonify([d.to_json() for d in drugs])


@app.route("/get_targets_for_drug", methods=["POST"])
def targets_for_drug():
    """Get targets for a drug."""
    drug = _post_request_preproc("drug")
    targets = get_targets_for_drug(drug=drug, client=client)
    return jsonify([t.to_json() for t in targets])


@app.route("/is_drug_target", methods=["POST"])
def drug_target():
    """Check if the drug targets the given protein."""
    drug, target = _post_request_preproc("drug", "target")
    return jsonify(
        {"is_drug_target": is_drug_target(drug=drug, target=target, client=client)}
    )


cli = make_web_command(app=app)


if __name__ == "__main__":
    cli()
