"""Curation app for INDRA CoGEx."""

import logging
import time
from typing import Any, Callable, List, Mapping, Optional, Tuple

import flask
from flask import Response, abort, redirect, render_template, url_for
from flask_jwt_extended import jwt_optional
from flask_wtf import FlaskForm
from indra.statements import Statement
from wtforms import BooleanField, StringField, SubmitField, TextAreaField
from wtforms.validators import DataRequired

from indra_cogex.apps import proxies
from indra_cogex.apps.proxies import client, curation_cache
from indra_cogex.client.curation import (
    get_disprot_statements,
    get_dub_statements,
    get_entity_evidence_counts,
    get_goa_evidence_counts,
    get_kinase_statements,
    get_mirna_statements,
    get_phosphatase_statements,
    get_ppi_evidence_counts,
    get_tf_statements,
)
from indra_cogex.client.queries import get_stmts_for_mesh, get_stmts_for_stmt_hashes

from .utils import get_conflict_evidence_counts
from ..utils import (
    remove_curated_evidences,
    remove_curated_pa_hashes,
    remove_curated_statements,
    render_statements,
)
from ...client import get_stmts_for_paper, indra_subnetwork, indra_subnetwork_go

__all__ = [
    "curator_blueprint",
]

logger = logging.getLogger(__name__)
curator_blueprint = flask.Blueprint("curator", __name__, url_prefix="/curate")

EVIDENCE_TEXT = """\
Statements are listed in descending order by number of textual evidences
such that entries appearing earlier should be easier to curate.
"""


def _database_text(s: str) -> str:
    return f"""\
    INDRA statements already
    appearing in high-quality reference databases like {s}
    and statements that have already been curated are filtered out such
    that only novel, potentially interesting statements are displayed.
    """


class GeneOntologyForm(FlaskForm):
    """A form for choosing a GO term."""

    term = StringField(
        "Gene Ontology Term",
        validators=[DataRequired()],
        description="Choose a gene ontology term to curate (e.g., "
        '<a href="./GO:0003677">GO:0003677</a> for Apoptotic Process)',
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
    return _enrich_render_statements(
        stmts,
        title=f"GO Curator: {term}",
        description=f"""\
            The GO Pathway curator identifies a list of genes associated with
            the given GO term then INDRA statements where the subject and
            object are both from the list using INDRA CoGEx.
            {_database_text("Pathway Commons")}
            {EVIDENCE_TEXT}
        """,
    )


def _enrich_render_statements(
    stmts: List[Statement],
    title: str,
    description: str,
    curations: Optional[List[Mapping[str, Any]]] = None,
) -> Response:
    if curations is None:
        curations = curation_cache.get_curations()
    stmts = remove_curated_statements(stmts, curations=curations)
    stmts = stmts[: proxies.limit]

    logger.info(f"Enriching {len(stmts)} statements")
    start_time = time.time()

    enriched_stmts, evidence_counts = get_stmts_for_stmt_hashes(
        [stmt.get_hash() for stmt in stmts],
        evidence_limit=10,
        return_evidence_counts=True,
    )
    evidence_lookup_time = time.time() - start_time
    logger.info(f"Got statements in {evidence_lookup_time:.2f} seconds")
    return render_statements(
        enriched_stmts,
        title=title,
        evidence_counts=evidence_counts,
        evidence_lookup_time=evidence_lookup_time,
        curations=curations,
        description=description,
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


MESH_CURATION_SUBSETS = {
    "ppi": ("hgnc", "hgnc"),
    "pmi": ("hgnc", "chebi"),
    "go": ("hgnc", "go"),
}


@curator_blueprint.route("/mesh/<term>", methods=["GET"])
@curator_blueprint.route("/mesh/<term>/<subset>", methods=["GET"])
@jwt_optional
def curate_mesh(term: str, subset: Optional[str] = None):
    """Curate all statements for papers with a given MeSH annotation."""
    if subset is None:
        return _curate_mesh_helper(term=term)
    elif subset not in MESH_CURATION_SUBSETS:
        return abort(
            400,
            f"Invalid subset: {subset}. Choose one of {sorted(MESH_CURATION_SUBSETS)}",
        )
    else:
        subject_prefix, object_prefix = MESH_CURATION_SUBSETS[subset]
        return _curate_mesh_helper(
            term=term, subject_prefix=subject_prefix, object_prefix=object_prefix
        )


def _curate_mesh_helper(
    term: str,
    subject_prefix: Optional[str] = None,
    object_prefix: Optional[str] = None,
    filter_curated: bool = True,
    curations: Optional[List[Mapping[str, Any]]] = None,
) -> Response:
    if curations is None:
        logger.info("Getting curations")
        curations = curation_cache.get_curations()
        logger.debug(f"Got {len(curations)} curations")

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
        stmts = remove_curated_statements(stmts, curations=curations)

    return render_statements(
        stmts,
        title=f"MeSH Curator: {term}",
        evidence_counts=evidence_counts,
        evidence_lookup_time=evidence_lookup_time,
        limit=proxies.limit,
        curations=curations,
        description=f"""\
            The topic curator identifies INDRA Statements in publications
            annotated with the given Medical Subject Headings (MeSH) term
            using INDRA CoGEx. INDRA statements already appearing in
            high-quality reference databases and statements that have already
            been curated are filtered out such that only novel, potentially
            interesting statements are displayed. {EVIDENCE_TEXT}
        """,
    )


def _render_func(
    func: Callable[..., Mapping[int, int]],
    *,
    title: str,
    description: str,
    func_kwargs: Optional[Mapping[str, Any]] = None,
    **kwargs,
) -> Response:
    """Render the evidence counts generated by a function call.

    Parameters
    ----------
    func :
        A function that takes a ``client`` and any arbitrary arguments
        (passed through ``func_kwargs``) and returns an evidence count
        dictionary
    func_kwargs :
        Keyword arguments to pass to the function
    title :
        The title of the page
    description :
        The description text to show on the page
    kwargs :
        Remaining keyword arguments to forward to :func:`_render_evidence_counts`

    Returns
    -------
    :
        A Flask response generated by calling the function then rendering
        the statements is evidence count dictionary covers
    """
    start = time.time()
    evidence_counts = func(client=client, **(func_kwargs or {}))
    time_delta = time.time() - start
    logger.info(
        f"got evidence counts for {len(evidence_counts)} statements in {time_delta:.2f} seconds."
    )
    return _render_evidence_counts(
        evidence_counts,
        title=title,
        description=description,
        **kwargs,
    )


def _render_evidence_counts(
    evidence_counts: Mapping[int, int],
    title: str,
    filter_curated: bool = True,
    description: Optional[str] = None,
) -> Response:
    curations = curation_cache.get_curations()
    logger.debug(f"loaded {len(curations):,} curations")
    # Prepare prioritized statement hash list sorted by decreasing evidence count
    pa_hashes = sorted(evidence_counts, key=evidence_counts.get, reverse=True)
    if filter_curated:
        pa_hashes = remove_curated_pa_hashes(pa_hashes, curations=curations)
        total_uncurated_statements = len(pa_hashes)
        logger.debug(
            f"filtered to {total_uncurated_statements:,} statements ({len(pa_hashes) - total_uncurated_statements:,} were removed)"
        )
    pa_hashes = pa_hashes[: proxies.limit]

    start_time = time.time()
    stmts = get_stmts_for_stmt_hashes(pa_hashes, evidence_limit=10, client=client)
    evidence_lookup_time = time.time() - start_time
    logger.info(f"Got statements in {evidence_lookup_time:.2f} seconds")

    return render_statements(
        stmts,
        title=title,
        evidence_counts=evidence_counts,
        evidence_lookup_time=evidence_lookup_time,
        curations=curations,
        description=description,
        # no limit necessary here since it was already applied above
    )


@curator_blueprint.route("/ppi", methods=["GET"])
@jwt_optional
def ppi():
    """The PPI curator looks for the highest evidences for PPIs that don't appear in a database."""
    return _render_func(
        get_ppi_evidence_counts,
        title="PPI Curator",
        description=f"""\
            The protein-protein interaction (PPI) curator identifies INDRA
            statements using INDRA CoGEx whose subjects and objects are human
            gene products (i.e., RNA or proteins) and whose statements are
            "binds". 
            {_database_text("BioGRID, SIGNOR, and Pathway Commons")}
            {EVIDENCE_TEXT}
        """,
    )


@curator_blueprint.route("/goa", methods=["GET"])
@jwt_optional
def goa():
    """The GO Annotation curator looks for the highest evidence gene-GO term relations that don't appear in GOA."""
    return _render_func(
        get_goa_evidence_counts,
        title="GO Annotation Curator",
        description=f"""\
            The Gene Ontology annotation curator identifiers INDRA statements
            using INDRA CoGEx whose subjects are human genes/proteins and whose
            objects are Gene Ontology terms. Statements whose gene-GO term pair
            already appear in the Gene Ontology Annotation database and statements
            that have already been curated are filtered out such that only novel,
            potentially interesting statements are displayed. 
            {EVIDENCE_TEXT}
        """,
    )


@curator_blueprint.route("/conflicts", methods=["GET"])
@jwt_optional
def conflicts():
    """Curate statements with conflicting prior curations."""
    return _render_func(
        get_conflict_evidence_counts, title="Conflict Resolver", filter_curated=False
    )


@curator_blueprint.route("/tf", methods=["GET"])
@jwt_optional
def tf():
    """Curate transcription factors."""
    return _render_func(
        get_tf_statements,
        title="Transcription Factor Curator",
        description=f"""\
            The transcription factor curator identifies INDRA statements using
            INDRA CoGEx whose subjects are human transcription factors and whose
            statements are "increases amount of" or "decreases amount of".
            {_database_text("Pathway Commons")}
            {EVIDENCE_TEXT}
        """,
    )


@curator_blueprint.route("/kinase", methods=["GET"])
@jwt_optional
def kinase():
    """Curate kinases."""
    return _render_func(
        get_kinase_statements,
        title="Kinase Curator",
        description=f"""\
            The kinase curator identifies INDRA statements using INDRA
            CoGEx whose subjects are human protein kinases and whose
            statements are "phosphorylates". 
            {_database_text("PhosphoSitePlus")}
            {EVIDENCE_TEXT}
        """,
    )


@curator_blueprint.route("/phosphatase", methods=["GET"])
@jwt_optional
def phosphatase():
    """Curate phosphatases."""
    return _render_func(
        get_phosphatase_statements,
        title="Phosphatase Curator",
        description=f"""\
            The phosphatase curator identifies INDRA statements using INDRA
            CoGEx whose subjects are human phosphatase genes and whose
            statements are "dephosphorylates".
            {_database_text("Pathway Commons")}
            {EVIDENCE_TEXT}
        """,
    )


@curator_blueprint.route("/dub", methods=["GET"])
@jwt_optional
def deubiquitinase():
    """Curate deubiquitinases."""
    return _render_func(
        get_dub_statements,
        title="Deubiquitinase Curator",
        description=f"""\
            The deubiquitinase curator identifies INDRA statements using INDRA
            CoGEx whose subjects are human deubiquitinase genes and whose
            statements are "deubiquinates".
            {_database_text("Pathway Commons")}
            {EVIDENCE_TEXT}
        """,
    )


@curator_blueprint.route("/mirna", methods=["GET"])
@jwt_optional
def mirna():
    """Curate miRNAs."""
    return _render_func(
        get_mirna_statements,
        title="miRNA Curator",
        description=f"""\
            The miRNA curator identifies INDRA statements using INDRA
            CoGEx whose subjects are micro-RNAs and whose
            statements are "increases amount" or "decreases amount".
            {_database_text("miRTarBase")}
            {EVIDENCE_TEXT}
        """,
    )


@curator_blueprint.route("/disprot", methods=["GET"])
@curator_blueprint.route("/disprot/<object_prefix>", methods=["GET"])
@jwt_optional
def disprot(object_prefix: Optional[str] = None):
    """Curate intrensically disordered proteins."""
    assert object_prefix in {None, "hgnc", "go", "chebi"}
    return _render_func(
        get_disprot_statements,
        func_kwargs=dict(object_prefix=object_prefix),
        title="DisProt Curator",
        description=f"""\
            The DisProt curator identifies INDRA statements using INDRA
            CoGEx whose subjects are intrensically disordered proteins.
            {EVIDENCE_TEXT}
        """,
    )


@curator_blueprint.route("/modulator/", methods=["GET"])
@jwt_optional
def modulator():
    """Get small molecule modulators for the given protein."""
    raise NotImplementedError


@curator_blueprint.route("/entity/<prefix>:<identifier>", methods=["GET"])
@jwt_optional
def entity(prefix: str, identifier: str):
    """Get all statements about the given entity."""
    if prefix in {"pubmed", "pmc", "doi", "trid"}:
        return _curate_paper(prefix, identifier, filter_curated=proxies.filter_curated)
    if prefix in {"hgnc"}:
        return _render_func(
            get_entity_evidence_counts,
            func_kwargs=dict(
                prefix=prefix,
                identifier=identifier,
            ),
            title="Entity Curator",
            description=f"""\
                Curate statements where <code>{prefix}:{identifier}</code> is the
                subject.
                {_database_text("Pathway Commons")}
                {EVIDENCE_TEXT}
            """,
        )
    else:
        return abort(404, f"Unhandled prefix: {prefix}")


class PaperForm(FlaskForm):
    """A form for choosing a MeSH disease term."""

    identifier = StringField(
        "Publication identifier or CURIE",
        validators=[DataRequired()],
        description="""\
            This field accepts identifiers from PubMed, PubMed Central, and DOI
            as either a CURIEs that looks like <code>pubmed:1234</code>,
            <code>pmc:PMC3084216</code>, or <code>doi:10.1038/nbt1156</code>
            <strong>or</strong> as a local unique identifier that looks like
            <code>1234</code>, <code>PMC3084216</code>, or
            <code>10.1038/nbt1156</code>.
        """,
    )
    filter_curated = BooleanField(
        "Filter Curated Evidences",
        default=True,
        description="Do not show evidences that have been previously curated",
    )
    submit = SubmitField("Submit")

    def get_term(self) -> Tuple[str, str]:
        s: str = self.identifier.data
        if s.isnumeric():
            return "pubmed", s
        if s.startswith("PMC"):
            return "pmc", s
        if "." in s:
            return "doi", s
        if ":" not in s:
            raise ValueError(f"Can not prefix for {s}. Consider writing as a CURIE.")
        prefix, identifier = s.split(":", 1)
        prefix = prefix.lower()
        if prefix in {"pmid", "pubmed"}:
            return "pubmed", identifier
        if prefix == "doi":
            return "doi", identifier
        if prefix in {"pmc", "pmcid"}:
            if identifier.startswith("PMC"):
                return "pmc", identifier
            elif identifier.isnumeric():
                return "pmc", f"PMC{identifier}"
            else:
                raise ValueError
        if prefix == "trid":
            return prefix, identifier
        raise ValueError(f"Unhandled prefix in CURIE {s}")


@curator_blueprint.route("/paper", methods=["GET", "POST"])
@jwt_optional
def paper():
    """Get all statements for the given paper."""
    form = PaperForm()
    if not form.is_submitted():
        return render_template("curation/paper_form.html", form=form)

    prefix, identifier = form.get_term()

    if form.filter_curated.data:
        url = url_for(f".{entity.__name__}", prefix=prefix, identifier=identifier)
    else:
        url = url_for(
            f".{entity.__name__}",
            prefix=prefix,
            identifier=identifier,
            filter_curated=False,
        )
    return redirect(url)


def _curate_paper(
    prefix: str,
    identifier: str,
    filter_curated: bool = True,
    curations: Optional[List[Mapping[str, Any]]] = None,
) -> Response:
    stmts, evidence_counts = get_stmts_for_paper(
        (prefix, identifier), return_evidence_counts=True
    )
    if curations is None:
        curations = curation_cache.get_curations()
    if filter_curated:
        stmts = remove_curated_evidences(stmts, curations=curations)
    return render_statements(
        stmts,
        title=f"Publication Curator: {prefix}:{identifier}",
        evidence_counts=evidence_counts,
        limit=proxies.limit,
        curations=curations,
        description=f"""
            Curate statements with evidences occurring in 
            <a href="https://bioregistry.io/{prefix}:{identifier}">
            <code>{prefix}:{identifier}</code></a>.
        """,
        endpoint=f".{entity.__name__}",
        prefix=prefix,
        identifier=identifier,
        filter_curated=filter_curated,
    )


class NodesForm(FlaskForm):
    """A form for inputting multiple nodes."""

    curies = TextAreaField(
        "Biomedical Entity CURIEs",
        validators=[DataRequired()],
        description="Please enter INDRA-flavored CURIEs separated by commas or new lines.",
    )
    submit = SubmitField("Submit")

    def get_nodes(self) -> List[Tuple[str, str]]:
        """Get the CURIEs from the form."""
        return sorted(
            {
                tuple(entry.strip().split(":", 1))
                for line in self.curies.data.split("\n")
                for entry in line.strip().split(",")
            }
        )


@curator_blueprint.route("/subnetwork", methods=["GET", "POST"])
@jwt_optional
def subnetwork():
    """Get all statements induced by the nodes."""
    form = NodesForm()
    if not form.is_submitted():
        return render_template("curation/node_form.html", form=form)

    nodes = form.get_nodes()
    if len(nodes) > 30:
        flask.flash("Can not query more than 30 nodes.")
        return render_template("curation/node_form.html", form=NodesForm())

    nodes_html = " ".join(
        f"""\
        <a class="badge badge-info" href="https://bioregistry.io/{prefix}:{identifier}" target="_blank">
            {prefix}:{identifier}
        </a>"""
        for prefix, identifier in nodes
    )

    stmts = indra_subnetwork(nodes=nodes, client=client)
    return _enrich_render_statements(
        stmts,
        title="Subnetwork Curator",
        description=f"""\
        The subnetwork curator shows statements between the following nodes.
        {_database_text("Pathway Commons")}
        {EVIDENCE_TEXT}
        </p>
        <p>
        {nodes_html}
        """,
    )


@curator_blueprint.route("/statement/<int:stmt_hash>", methods=["GET"])
@jwt_optional
def curate_statement(stmt_hash: int):
    """Curate all evidences for the statement."""
    start_time = time.time()
    enriched_stmts, evidence_counts = get_stmts_for_stmt_hashes(
        [stmt_hash],
        evidence_limit=10,
        return_evidence_counts=True,
    )
    evidence_lookup_time = time.time() - start_time
    logger.info(f"Got statements in {evidence_lookup_time:.2f} seconds")
    # TODO either auto-expand or use a slightly different template
    return render_statements(
        enriched_stmts,
        title=f"Statement Curator: {stmt_hash}",
        evidence_counts=evidence_counts,
        evidence_lookup_time=evidence_lookup_time,
        description="Curate evidences from a single statement",
    )
