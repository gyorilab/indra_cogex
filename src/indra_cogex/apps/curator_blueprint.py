"""Curation app for INDRA CoGEx."""

import logging
from typing import Iterable

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
    get_mesh_disease_curation_hashes,
    get_ppi_hashes,
)
from indra_cogex.client.queries import get_stmts_for_stmt_hashes

from .utils import render_statements, resolve_email

__all__ = [
    "curator_blueprint",
]

logger = logging.getLogger(__name__)
curator_blueprint = flask.Blueprint("curator", __name__, url_prefix="/curate")


class GeneOntologyForm(FlaskForm):
    """A form for choosing a GO term."""

    term = StringField(
        "Gene Ontology Term",
        validators=[DataRequired()],
        description="Choose a gene ontology term to curate (e.g., GO:0003677)",
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
    hashes = get_go_curation_hashes(go_term=("GO", term), client=client)
    return _render_hashes(hashes, title=f"GO Curator: {term}")


class MeshDiseaseForm(FlaskForm):
    """A form for choosing a MeSH disease term."""

    term = StringField(
        "MeSH Term",
        validators=[DataRequired()],
        description="Choose a MeSH disease to curate (e.g., D006009)",
    )
    submit = SubmitField("Submit")


@curator_blueprint.route("/mesh-disease/", methods=["GET", "POST"])
def mesh_disease():
    """A home page for MeSH Disease curation."""
    form = MeshDiseaseForm()
    if form.is_submitted():
        return redirect(url_for(f".{curate_mesh.__name__}", term=form.term.data))
    return render_template("curation/mesh_form.html", form=form)


@curator_blueprint.route("/mesh-disease/<term>", methods=["GET"])
@jwt_optional
def curate_mesh(term: str):
    hashes = get_mesh_disease_curation_hashes(mesh_term=("MESH", term), client=client)
    return _render_hashes(hashes, title=f"MeSH Curator: {term}")


def _render_hashes(hashes: Iterable[int], title: str) -> Response:
    stmts = get_stmts_for_stmt_hashes(hashes[: proxies.limit])
    _, _, email = resolve_email()
    return render_statements(stmts, user_email=email, title=title)


@curator_blueprint.route("/ppi", methods=["GET"])
@jwt_optional
def ppi():
    """The PPI curator looks for the highest evidences for PPIs that don't appear in a database."""
    hashes = get_ppi_hashes(client=client, limit=proxies.limit)
    return _render_hashes(hashes, title="PPI Curator")
