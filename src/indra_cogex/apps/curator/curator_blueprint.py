"""Curation app for INDRA CoGEx."""

import logging
import time
from typing import Mapping, Optional

import flask
from flask import Response, redirect, render_template, url_for
from flask_jwt_extended import jwt_optional
from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField
from wtforms.validators import DataRequired

from indra_cogex.apps import proxies
from indra_cogex.apps.proxies import client
from indra_cogex.client.curation import (
    get_go_curation_hashes,
    get_goa_evidence_counts,
    get_kinase_statements,
    get_phosphatase_statements,
    get_ppi_evidence_counts,
    get_tf_statements,
)
from indra_cogex.client.queries import get_stmts_for_mesh, get_stmts_for_stmt_hashes

from .utils import get_conflict_evidence_counts
from ..utils import (
    remove_curated_pa_hashes,
    remove_curated_statements,
    render_statements,
)

__all__ = [
    "curator_blueprint",
]

from ...client import indra_subnetwork_go

logger = logging.getLogger(__name__)
curator_blueprint = flask.Blueprint("curator", __name__, url_prefix="/curate")


class GeneOntologyForm(FlaskForm):
    """A form for choosing a GO term."""

    term = StringField(
        "Gene Ontology Term",
        validators=[DataRequired()],
        description='Choose a gene ontology term to curate (e.g., <a href="./GO:0003677">GO:0003677</a> for Apoptotic Process)',
    )
    submit = SubmitField("Submit")


@curator_blueprint.route("/go/", methods=["GET", "POST"])
def gene_ontology():
    """A home page for GO curation."""
    form = GeneOntologyForm()
    if form.is_submitted():
        return redirect(url_for(f".{curate_go.__name__}", term=form.term.data))
    return render_template("curation/go_form.html", form=form)


@curator_blueprint.route("/go/<term>", methods=["GET"])
@jwt_optional
def curate_go(term: str):
    stmts = indra_subnetwork_go(
        go_term=("GO", term),
        client=client,
    )
    stmts = remove_curated_statements(stmts)
    stmts = stmts[: proxies.limit]

    logger.info(f"Enriching {len(stmts)} statements")
    start_time = time.time()
    stmts, evidence_counts = get_stmts_for_stmt_hashes(
        pa_hashes, evidence_limit=10, return_evidence_counts=True
    )
    evidence_lookup_time = time.time() - start_time
    logger.info(f"Got statements in {evidence_lookup_time:.2f} seconds")
    return render_statements(
        stmts,
        title=f"GO Curator: {term}",
        evidence_counts=evidence_counts,
        evidence_lookup_time=evidence_lookup_time,
        # no limit necessary here since it was already applied above
    )


class MeshDiseaseForm(FlaskForm):
    """A form for choosing a MeSH disease term."""

    term = StringField(
        "MeSH Term",
        validators=[DataRequired()],
        description='Choose a MeSH disease to curate (e.g., <a href="./D006009">D006009</a> for Pompe Disease)',
    )
    submit = SubmitField("Submit")


@curator_blueprint.route("/mesh/", methods=["GET", "POST"])
def mesh():
    """A home page for MeSH Disease curation."""
    form = MeshDiseaseForm()
    if form.is_submitted():
        return redirect(url_for(f".{curate_mesh.__name__}", term=form.term.data))
    return render_template("curation/mesh_form.html", form=form)


@curator_blueprint.route("/mesh/<term>", methods=["GET"])
@jwt_optional
def curate_mesh(term: str):
    """Curate all statements for papers with a given MeSH annotation."""
    return _curate_mesh_helper(term=term)


@curator_blueprint.route("/mesh/<term>/ppi", methods=["GET"])
@jwt_optional
def curate_mesh_ppis(term: str):
    """Curate protein-protein statements for papers with a given MeSH annotation."""
    return _curate_mesh_helper(term=term, subject_prefix="hgnc", object_prefix="hgnc")


@curator_blueprint.route("/mesh/<term>/pmi", methods=["GET"])
@jwt_optional
def curate_mesh_pmi(term: str):
    """Curate protein-metabolite statements for papers with a given MeSH annotation."""
    return _curate_mesh_helper(term=term, subject_prefix="hgnc", object_prefix="chebi")


def _curate_mesh_helper(
    term: str,
    subject_prefix: Optional[str] = None,
    object_prefix: Optional[str] = None,
    filter_curated: bool = True,
) -> Response:
    logger.info(f"Getting statements for mesh:{term}")
    start_time = time.time()
    stmts, evidence_counts = get_stmts_for_mesh(
        mesh_term=("MESH", term),
        include_child_terms=True,
        client=client,
        return_evidence_counts=True,
        evidence_limit=10,
        subject_prefix=subject_prefix,
        object_prefix=object_prefix,
    )
    evidence_lookup_time = time.time() - start_time

    if filter_curated:
        stmts = remove_curated_statements(stmts)

    return render_statements(
        stmts,
        title=f"MeSH Curator: {term}",
        evidence_counts=evidence_counts,
        evidence_lookup_time=evidence_lookup_time,
        limit=proxies.limit,
    )


def _render_evidence_counts(
    evidence_counts: Mapping[int, int], title: str, filter_curated: bool = True
) -> Response:
    # Prepare prioritized statement hash list sorted by decreasing evidence count
    pa_hashes = sorted(evidence_counts, key=evidence_counts.get, reverse=True)
    if filter_curated:
        pa_hashes = remove_curated_pa_hashes(pa_hashes)
    pa_hashes = pa_hashes[: proxies.limit]

    start_time = time.time()
    stmts = get_stmts_for_stmt_hashes(pa_hashes, evidence_limit=10)
    evidence_lookup_time = time.time() - start_time
    logger.info(f"Got statements in {evidence_lookup_time:.2f} seconds")

    return render_statements(
        stmts,
        title=title,
        evidence_counts=evidence_counts,
        evidence_lookup_time=evidence_lookup_time,
        # no limit necessary here since it was already applied above
    )


@curator_blueprint.route("/ppi", methods=["GET"])
@jwt_optional
def ppi():
    """The PPI curator looks for the highest evidences for PPIs that don't appear in a database."""
    evidence_counts = get_ppi_evidence_counts(client=client)
    return _render_evidence_counts(evidence_counts, title="PPI Curator")


@curator_blueprint.route("/goa", methods=["GET"])
@jwt_optional
def goa():
    """The GO Annotation curator looks for the highest evidence gene-GO term relations that don't appear in GOA."""
    evidence_counts = get_goa_evidence_counts(client=client, limit=proxies.limit)
    return _render_evidence_counts(evidence_counts, title="GO Annotation Curator")


@curator_blueprint.route("/conflicts", methods=["GET"])
@jwt_optional
def conflicts():
    """Curate statements with conflicting prior curations."""
    evidence_counts = get_conflict_evidence_counts(client=client)
    return _render_evidence_counts(
        evidence_counts, title="Conflict Resolver", filter_curated=False
    )


@curator_blueprint.route("/tf", methods=["GET"])
@jwt_optional
def tf():
    """Curate transcription factors."""
    evidence_counts = get_tf_statements(client=client, limit=proxies.limit)
    return _render_evidence_counts(
        evidence_counts, title="Transcription Factor Curator"
    )


@curator_blueprint.route("/kinase", methods=["GET"])
@jwt_optional
def kinase():
    """Curate kinases."""
    evidence_counts = get_kinase_statements(client=client, limit=proxies.limit)
    return _render_evidence_counts(evidence_counts, title="Kinase Curator")


@curator_blueprint.route("/phosphatase", methods=["GET"])
@jwt_optional
def phosphatase():
    """Curate phosphatases."""
    evidence_counts = get_phosphatase_statements(client=client, limit=proxies.limit)
    return _render_evidence_counts(evidence_counts, title="Phosphatase Curator")
