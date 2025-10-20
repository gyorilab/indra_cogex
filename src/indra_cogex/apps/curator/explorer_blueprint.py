"""Blueprint for INDRA CoGEx exploration and curation."""

import logging
import time
from typing import Any, Callable, List, Mapping, Optional, Tuple

import flask
from flask import Response, abort, redirect, render_template, url_for, jsonify, session
from flask_jwt_extended import jwt_required
from flask_wtf import FlaskForm
from flask import request
from indra.statements import Statement
from wtforms import BooleanField, StringField, SubmitField, TextAreaField
from wtforms.validators import DataRequired

from indra_cogex.apps import proxies
from indra_cogex.apps.proxies import client, curation_cache
from indra_cogex.client.curation import (
    get_disprot_statements,
    get_dub_statements,
    get_entity_source_counts,
    get_goa_source_counts,
    get_kinase_statements,
    get_mirna_statements,
    get_phosphatase_statements,
    get_ppi_source_counts,
    get_tf_statements,
)
from indra_cogex.client.queries import get_stmts_for_mesh, get_stmts_for_stmt_hashes, get_network
from indra_cogex.client import Neo4jClient
from .utils import get_conflict_source_counts
from ..utils import (
    remove_curated_pa_hashes,
    remove_curated_statements,
    render_statements,
)
from bioregistry import normalize_curie
from indra.databases import hgnc_client
from ...client import get_stmts_for_paper, indra_subnetwork, indra_subnetwork_go
from ...client.neo4j_client import process_identifier

__all__ = [
    "explorer_blueprint",
]

logger = logging.getLogger(__name__)
explorer_blueprint = flask.Blueprint("explorer", __name__, url_prefix="/explore")

EVIDENCE_TEXT = """\
Statements are listed in descending order by number of textual evidences
such that entries appearing earlier should be easier to explore.
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
        description="Choose a gene ontology term to explore (e.g., "
                    '<a href="#" onclick="event.preventDefault(); document.getElementById('
                    '\'term\').value=\'GO:0003677\'">GO:0003677</a> for Apoptotic Process)',
    )
    include_db_evidence = BooleanField('Include database evidence',
                                       default=True)
    submit = SubmitField("Submit")


@explorer_blueprint.route("/go/", methods=["GET", "POST"])
def gene_ontology():
    """A home page for GO exploration."""
    form = GeneOntologyForm()
    if form.validate_on_submit():
        include_db_evidence = form.include_db_evidence.data
        return redirect(url_for(f".{explore_go.__name__}",
                                term=form.term.data,
                                include_db_evidence=str(include_db_evidence).lower()))
    return render_template("curation/go_form.html", form=form)


@explorer_blueprint.route("/go/<term>", methods=["GET"])
@jwt_required(optional=True)
def explore_go(term: str):
    include_db_evidence = request.args.get('include_db_evidence', 'false').lower() == 'true'
    stmts, source_counts = indra_subnetwork_go(
        go_term=("GO", term),
        client=client,
        include_db_evidence=include_db_evidence,
        order_by_ev_count=True,
        return_source_counts=True,
    )

    return _enrich_render_statements(
        stmts,
        title=f"GO Explorer: {term}",
        description=f"""\
            The GO Pathway explorer identifies a list of genes associated with
            the given GO term then INDRA statements where the subject and
            object are both from the list using INDRA CoGEx.
            {_database_text("Pathway Commons")}
            {EVIDENCE_TEXT}
        """,
        include_db_evidence=include_db_evidence,
        source_counts=source_counts,
        prefix="go",
        identifier=term,
        store_hashes_in_session=True
    )


def _enrich_render_statements(
    stmts: List[Statement],
    title: str,
    description: str,
    curations: Optional[List[Mapping[str, Any]]] = None,
    no_stmts_message: Optional[str] = None,
    include_db_evidence: bool = False,
    source_counts: Optional[Mapping[int, Mapping[str, int]]] = None,
    prefix: Optional[str] = None,
    store_hashes_in_session: bool = False,
    identifier: Optional[str] = None,
) -> Response:
    if curations is None:
        curations = curation_cache.get_curation_cache()
    stmts = remove_curated_statements(stmts, curations=curations, include_db_evidence=include_db_evidence)

    # Apply selective limits based on endpoint
    if request.endpoint == 'explorer.go_term':
        # GO Explorer gets limit of 100
        stmts = stmts[:100]
    elif request.endpoint == 'explorer.subnetwork':
        # Subnetwork Explorer gets no limit
        pass  # Don't apply any limit
    else:
        # Other explorers that use this function keep original behavior
        stmts = stmts[:proxies.limit]

    logger.info(f"Enriching {len(stmts)} statements")
    start_time = time.time()

    enriched_stmts, evidence_counts = get_stmts_for_stmt_hashes(
        [stmt.get_hash() for stmt in stmts],
        evidence_limit=10,
        return_evidence_counts=True,
        include_db_evidence=include_db_evidence,
    )
    evidence_lookup_time = time.time() - start_time
    logger.info(f"Got statements in {evidence_lookup_time:.2f} seconds")
    return render_statements(
        enriched_stmts,
        title=title,
        evidence_counts=evidence_counts,
        evidence_lookup_time=evidence_lookup_time,
        curations=curations,
        source_counts_dict=source_counts,
        description=description,
        no_stmts_message=no_stmts_message,
        include_db_evidence=include_db_evidence,
        prefix=prefix,
        store_hashes_in_session=store_hashes_in_session,
        identifier=identifier
        # no limit necessary here since it was already applied above
    )


class MeshDiseaseForm(FlaskForm):
    """A form for choosing a MeSH disease term."""

    term = StringField(
        "MeSH Term",
        validators=[DataRequired()],
        description='Choose a MeSH disease to explore (e.g., <a href="#" onclick="event.preventDefault(); '
                    'document.getElementById(\'term\').value=\'D006009\'">D006009</a> for Pompe Disease)',
    )
    include_db_evidence = BooleanField('Include database evidence',
                                       default=True)
    submit = SubmitField("Submit")


@explorer_blueprint.route("/mesh/", methods=["GET", "POST"])
def mesh():
    """A home page for MeSH Disease exploration."""
    form = MeshDiseaseForm()
    if form.validate_on_submit():
        include_db_evidence = form.include_db_evidence.data
        return redirect(url_for(f".{explore_mesh.__name__}",
                                term=form.term.data,
                                include_db_evidence=str(include_db_evidence).lower()))
    return render_template("curation/mesh_form.html", form=form)


MESH_EXPLORATION_SUBSETS = {
    "ppi": ("hgnc", "hgnc"),
    "pmi": ("hgnc", "chebi"),
    "go": ("hgnc", "go"),
}


@explorer_blueprint.route("/mesh/<term>", methods=["GET"])
@explorer_blueprint.route("/mesh/<term>/<subset>", methods=["GET"])
@jwt_required(optional=True)
def explore_mesh(term: str, subset: Optional[str] = None):
    """Explore all statements for papers with a given MeSH annotation."""
    include_db_evidence = request.args.get('include_db_evidence', 'false').lower() == 'true'
    if subset is None:
        return _explore_mesh_helper(term=term, include_db_evidence=include_db_evidence)
    elif subset not in MESH_EXPLORATION_SUBSETS:
        return abort(
            400,
            f"Invalid subset: {subset}. Choose one of {sorted(MESH_EXPLORATION_SUBSETS)}",
        )
    else:
        subject_prefix, object_prefix = MESH_EXPLORATION_SUBSETS[subset]
        return _explore_mesh_helper(
            term=term,
            subject_prefix=subject_prefix,
            object_prefix=object_prefix,
            include_db_evidence=include_db_evidence
        )


def _explore_mesh_helper(
    term: str,
    subject_prefix: Optional[str] = None,
    object_prefix: Optional[str] = None,
    filter_curated: bool = True,
    curations: Optional[List[Mapping[str, Any]]] = None,
    include_db_evidence: bool = False,
) -> Response:
    if curations is None:
        logger.info("Getting curations")
        curations = curation_cache.get_curation_cache()
        logger.debug(f"Got {len(curations)} curations")

    logger.info(f"Getting statements for mesh:{term}")
    start_time = time.time()
    stmts = get_stmts_for_mesh(
            mesh_term=("MESH", term),
            include_child_terms=True,
            client=client,
            evidence_limit=10,
            subject_prefix=subject_prefix,
            object_prefix=object_prefix,
            include_db_evidence=include_db_evidence,
    )

    evidence_lookup_time = time.time() - start_time

    if filter_curated:
        stmts = remove_curated_statements(stmts, curations=curations)

    return render_statements(
        stmts,
        title=f"MeSH Explorer: {term}",
        evidence_lookup_time=evidence_lookup_time,
        limit=proxies.limit,
        curations=curations,
        description=f"""\
            The topic explorer identifies INDRA Statements in publications
            annotated with the given Medical Subject Headings (MeSH) term
            using INDRA CoGEx. INDRA statements already appearing in
            high-quality reference databases and statements that have already
            been curated are filtered out such that only novel, potentially
            interesting statements are displayed. {EVIDENCE_TEXT}
        """,
        include_db_evidence=include_db_evidence
    )


def _render_func(
    func: Callable[..., Mapping[int, Mapping[str, int]]],
    *,
    title: str,
    description: str,
    func_kwargs: Optional[Mapping[str, Any]] = None,
    is_proteocentric=False,
    **kwargs,
) -> Response:
    """Render the evidence counts generated by a function call.

    Parameters
    ----------
    func :
        A function that takes a ``client`` and any arbitrary arguments
        (passed through ``func_kwargs``) and returns a source count
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
    include_db_evidence = request.args.get('include_db_evidence',
                                           'true' if is_proteocentric else 'true').lower() == 'true'
    func_kwargs = func_kwargs or {}
    func_kwargs['include_db_evidence'] = include_db_evidence

    start = time.time()
    stmt_hash_to_source_counts = func(client=client, **(func_kwargs or {}))
    time_delta = time.time() - start
    logger.info(
        f"got evidence counts for {len(stmt_hash_to_source_counts)} statements in {time_delta:.2f} seconds."
    )
    render_kwargs = {
        'stmt_hash_to_source_counts': stmt_hash_to_source_counts,
        'title': title,
        'description': description,
        'include_db_evidence': include_db_evidence,
        'is_proteocentric': is_proteocentric,
    }
    render_kwargs.update(kwargs)

    return _render_evidence_counts(**render_kwargs)


def _render_evidence_counts(
    stmt_hash_to_source_counts: Mapping[int, Mapping[str, int]],
    title: str,
    filter_curated: bool = True,
    description: Optional[str] = None,
    include_db_evidence: bool = True,
    is_proteocentric=False,
) -> Response:
    curations = curation_cache.get_curation_cache()
    logger.debug(f"loaded {len(curations):,} curations")
    evidence_counts = {
        stmt_hash: sum(source_counts.values())
        for stmt_hash, source_counts in stmt_hash_to_source_counts.items()
    }

    # Prepare prioritized statement hash list sorted by decreasing evidence count
    pa_hashes = sorted(evidence_counts, key=evidence_counts.get, reverse=True)
    if filter_curated:
        pa_hashes = remove_curated_pa_hashes(pa_hashes, curations=curations)
        total_uncurated_statements = len(pa_hashes)
        logger.debug(
            f"filtered to {total_uncurated_statements:,} statements ({len(pa_hashes) - total_uncurated_statements:,} "
            f"were removed)"
        )
    pa_hashes = pa_hashes[: proxies.limit]

    start_time = time.time()
    stmts = get_stmts_for_stmt_hashes(pa_hashes, evidence_limit=10, client=client,
                                      include_db_evidence=include_db_evidence)
    evidence_lookup_time = time.time() - start_time
    logger.info(f"Got statements in {evidence_lookup_time:.2f} seconds")

    return render_statements(
        stmts,
        title=title,
        evidence_counts=evidence_counts,
        evidence_lookup_time=evidence_lookup_time,
        curations=curations,
        description=description,
        source_counts_dict=stmt_hash_to_source_counts,
        include_db_evidence=include_db_evidence,
        is_proteocentric=is_proteocentric,
        # no limit necessary here since it was already applied above
    )


@explorer_blueprint.route("/ppi", methods=["GET"])
@jwt_required(optional=True)
def ppi():
    """The PPI explorer looks for the highest evidences for PPIs that don't appear in a database."""
    include_db_evidence = request.args.get('include_db_evidence', 'true').lower() == 'true'
    return _render_func(
        get_ppi_source_counts,
        title="PPI Explorer",
        description=f"""\
            The protein-protein interaction (PPI) explorer identifies INDRA
            statements using INDRA CoGEx whose subjects and objects are human
            gene products (i.e., RNA or proteins) and whose statements are
            "binds". 
            {_database_text("BioGRID, SIGNOR, and Pathway Commons")}
            {EVIDENCE_TEXT}
        """,
        func_kwargs={'include_db_evidence': include_db_evidence},
        is_proteocentric=True,
    )


@explorer_blueprint.route("/goa", methods=["GET"])
@jwt_required(optional=True)
def goa():
    """The GO Annotation explorer looks for the highest evidence gene-GO term relations that don't appear in GOA."""
    include_db_evidence = request.args.get('include_db_evidence', 'true').lower() == 'true'
    return _render_func(
        get_goa_source_counts,
        title="GO Annotation Explorer",
        description=f"""\
            The Gene Ontology annotation explorer identifiers INDRA statements
            using INDRA CoGEx whose subjects are human genes/proteins and whose
            objects are Gene Ontology terms. Statements whose gene-GO term pair
            already appear in the Gene Ontology Annotation database and statements
            that have already been curated are filtered out such that only novel,
            potentially interesting statements are displayed. 
            {EVIDENCE_TEXT}
        """,
        func_kwargs={'include_db_evidence': include_db_evidence},
        is_proteocentric=True,
    )


@explorer_blueprint.route("/conflicts", methods=["GET"])
@jwt_required(optional=True)
def conflicts():
    """Explore statements with conflicting prior curations."""
    return _render_func(
        get_conflict_source_counts, title="Conflict Resolver",
        filter_curated=False,
        description=f"""\
            The conflict resolver identifies INDRA statements that have
            conflicting prior curations. {_database_text("Pathway Commons")}
            {EVIDENCE_TEXT}
        """,
    )


@explorer_blueprint.route("/tf", methods=["GET"])
@jwt_required(optional=True)
def tf():
    """Explore transcription factors."""
    include_db_evidence = request.args.get('include_db_evidence', 'true').lower() == 'true'
    return _render_func(
        get_tf_statements,
        title="Transcription Factor Explorer",
        description=f"""\
            The transcription factor explorer identifies INDRA statements using
            INDRA CoGEx whose subjects are human transcription factors and whose
            statements are "increases amount of" or "decreases amount of".
            {_database_text("Pathway Commons")}
            {EVIDENCE_TEXT}
        """,
        func_kwargs={'include_db_evidence': include_db_evidence},
        is_proteocentric=True,
    )


@explorer_blueprint.route("/kinase", methods=["GET"])
@jwt_required(optional=True)
def kinase():
    """Explore kinases."""
    include_db_evidence = request.args.get('include_db_evidence', 'true').lower() == 'true'
    return _render_func(
        get_kinase_statements,
        title="Kinase Explorer",
        description=f"""\
            The kinase explorer identifies INDRA statements using INDRA
            CoGEx whose subjects are human protein kinases and whose
            statements are "phosphorylates". 
            {_database_text("PhosphoSitePlus")}
            {EVIDENCE_TEXT}
        """,
        func_kwargs={'include_db_evidence': include_db_evidence},
        is_proteocentric=True,
    )


@explorer_blueprint.route("/phosphatase", methods=["GET"])
@jwt_required(optional=True)
def phosphatase():
    """Explore phosphatases."""
    include_db_evidence = request.args.get('include_db_evidence', 'true').lower() == 'true'
    return _render_func(
        get_phosphatase_statements,
        title="Phosphatase Explorer",
        description=f"""\
            The phosphatase explorer identifies INDRA statements using INDRA
            CoGEx whose subjects are human phosphatase genes and whose
            statements are "dephosphorylates".
            {_database_text("Pathway Commons")}
            {EVIDENCE_TEXT}
        """,
        func_kwargs={'include_db_evidence': include_db_evidence},
        is_proteocentric=True,
    )


@explorer_blueprint.route("/dub", methods=["GET"])
@jwt_required(optional=True)
def deubiquitinase():
    """Explore deubiquitinases."""
    include_db_evidence = request.args.get('include_db_evidence', 'true').lower() == 'true'
    return _render_func(
        get_dub_statements,
        title="Deubiquitinase Explorer",
        description=f"""\
            The deubiquitinase explorer identifies INDRA statements using INDRA
            CoGEx whose subjects are human deubiquitinase genes and whose
            statements are "deubiquinates".
            {_database_text("Pathway Commons")}
            {EVIDENCE_TEXT}
        """,
        func_kwargs={'include_db_evidence': include_db_evidence},
        is_proteocentric=True,
    )


@explorer_blueprint.route("/mirna", methods=["GET"])
@jwt_required(optional=True)
def mirna():
    """Explore miRNAs."""
    include_db_evidence = request.args.get('include_db_evidence', 'true').lower() == 'true'
    return _render_func(
        get_mirna_statements,
        title="miRNA Explorer",
        description=f"""\
            The miRNA explorer identifies INDRA statements using INDRA
            CoGEx whose subjects are micro-RNAs and whose
            statements are "increases amount" or "decreases amount".
            {_database_text("miRTarBase")}
            {EVIDENCE_TEXT}
        """,
        func_kwargs={'include_db_evidence': include_db_evidence},
        is_proteocentric=True,
    )


@explorer_blueprint.route("/disprot", methods=["GET"])
@explorer_blueprint.route("/disprot/<object_prefix>", methods=["GET"])
@jwt_required(optional=True)
def disprot(object_prefix: Optional[str] = None):
    """Explore intrensically disordered proteins."""
    include_db_evidence = request.args.get('include_db_evidence', 'true').lower() == 'true'
    assert object_prefix in {None, "hgnc", "go", "chebi"}
    return _render_func(
        get_disprot_statements,
        title="DisProt Explorer",
        description=f"""\
            The DisProt explorer identifies INDRA statements using INDRA
            CoGEx whose subjects are intrensically disordered proteins.
            {EVIDENCE_TEXT}
        """,
        func_kwargs=dict(object_prefix=object_prefix, include_db_evidence=include_db_evidence),
        is_proteocentric=True,
    )


@explorer_blueprint.route("/modulator/", methods=["GET"])
@jwt_required(optional=True)
def modulator():
    """Get small molecule modulators for the given protein."""
    raise NotImplementedError


@explorer_blueprint.route("/entity/<prefix>:<path:identifier>", methods=["GET"])
@jwt_required(optional=True)
def entity(prefix: str, identifier: str):
    """Get all statements about the given entity."""
    include_db_evidence = request.args.get('include_db_evidence', 'false').lower() == 'true'
    if prefix in {"pubmed", "pmc", "doi", "trid"}:
        return _explore_paper(prefix, identifier, filter_curated=proxies.filter_curated,
                              include_db_evidence=include_db_evidence)
    if prefix in {"hgnc"}:
        return _render_func(
            get_entity_source_counts,
            func_kwargs=dict(
                prefix=prefix,
                identifier=identifier,
                include_db_evidence=include_db_evidence,
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
        description="""
                This field accepts identifiers from PubMed, PubMed Central, and DOI. Examples: 
                <a href="#" onclick="event.preventDefault(); 
                document.getElementById('identifier').value='pubmed:21040840'">pubmed:21040840</a>, 
                <a href="#" onclick="event.preventDefault(); 
                document.getElementById('identifier').value='PMC4617211'">PMC4617211</a>, or 
                <a href="#" onclick="event.preventDefault(); 
                document.getElementById('identifier').value='10.1016/J.CELL.2015.06.043'">
                doi:10.1016/J.CELL.2015.06.043</a>
                """
    )
    include_db_evidence = BooleanField(
        'Include database evidence',
        default=True,
        description="Include evidence from curated databases extracted from the given paper"
    )
    filter_curated = BooleanField(
        "Filter curated evidences",
        default=False,
        description="Do not show statements that have been curated for correctness",
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
            self.identifier.errors.append("Please provide identifier as a CURIE (e.g., pubmed:12345)")
            return None

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


@explorer_blueprint.route("/paper", methods=["GET", "POST"])
@jwt_required(optional=True)
def paper():
    """Get all statements for the given paper."""
    form = PaperForm()
    if form.validate_on_submit():
        result = form.get_term()
        if result is None:
            return render_template("curation/paper_form.html", form=form)
        prefix, identifier = form.get_term()
        include_db_evidence = form.include_db_evidence.data
        filter_curated = form.filter_curated.data

        return redirect(url_for(f".{entity.__name__}",
                                prefix=prefix,
                                identifier=identifier,
                                include_db_evidence=str(include_db_evidence).lower(),
                                filter_curated=str(filter_curated).lower()))
    return render_template("curation/paper_form.html", form=form)


def _explore_paper(
    prefix: str,
    identifier: str,
    filter_curated: bool = True,
    curations: Optional[List[Mapping[str, Any]]] = None,
    include_db_evidence: bool = False,
) -> Response:
    stmts = get_stmts_for_paper(
        (prefix, identifier), include_db_evidence=include_db_evidence,
    )
    if curations is None:
        curations = curation_cache.get_curation_cache()
    if filter_curated:
        stmts = remove_curated_statements(stmts, curations=curations, include_db_evidence=include_db_evidence)

    return render_statements(
        stmts,
        title=f"Publication Explorer: {prefix}:{identifier}",
        curations=curations,
        description=f"""
            Explore statements with evidences occurring in 
            <a href="https://bioregistry.io/{prefix}:{identifier}">
            <code>{prefix}:{identifier}</code></a>.
        """,
        endpoint=f".{entity.__name__}",
        prefix=prefix,
        identifier=identifier,
        filter_curated=filter_curated,
        store_hashes_in_session=True,
        include_db_evidence=include_db_evidence,
    )


class NodesForm(FlaskForm):
    """A form for inputting multiple nodes."""

    curies = TextAreaField(
        "Biomedical Entity CURIEs",
        validators=[DataRequired()],
        description="Please enter INDRA-flavored CURIEs separated by commas or new lines.",
    )
    include_db_evidence = BooleanField('Include Database Evidence',
                                       default=True)
    submit = SubmitField("Submit")

    def get_nodes(self) -> List[Tuple[str, str]]:
        """
        Collect raw node identifiers without strict CURIE validation.
        This lets the backend handle flexible normalization.
        """
        if not self.curies.data:
            return []

        # Split by newlines and commas
        entries = [
            entry.strip()
            for line in self.curies.data.split("\n")
            for entry in line.split(",")
            if entry.strip()
        ]

        # Just return them as raw strings
        return entries


def parse_node_list(node_list: List[str], client: Neo4jClient) -> Tuple[List[Tuple[str, str]], List[str]]:
    """Parse and normalize a list of node inputs into (prefix, identifier) tuples. """
    normalized_nodes = []
    errors = []

    for entry in node_list:
        entry = entry.strip()
        if not entry:
            continue

        # First, try Bioregistry normalization for CURIE-like inputs
        normalized = normalize_curie(entry)

        if normalized:
            # Successfully normalized by Bioregistry - split into prefix and identifier
            prefix, identifier = normalized.split(":", 1)
            normalized_nodes.append((prefix, identifier))
            continue

        # If Bioregistry didn't work, check if it's a human gene name
        # Try to get HGNC ID from gene symbol/name
        hgnc_id = hgnc_client.get_hgnc_id(entry)

        if hgnc_id:
            # Successfully found as a gene name
            normalized_nodes.append(("hgnc", hgnc_id))
            continue

        # If neither worked, add to errors
        errors.append(entry)

    return normalized_nodes, errors


@explorer_blueprint.route("/subnetwork", methods=["GET", "POST"])
@jwt_required(optional=True)
def subnetwork():
    """Get all statements induced by the nodes."""
    form = NodesForm()

    if form.validate_on_submit():  # Handle form submission
        # CLEAR OLD SESSION DATA BEFORE PROCESSING NEW SEARCH
        session.pop('statement_hashes', None)
        session.pop('subnetwork_input_nodes', None)
        session.pop('include_db_evidence', None)

        raw_nodes = form.get_nodes()  # Get raw strings from form
        logger.info(f"Raw input nodes: {raw_nodes}")

        if not raw_nodes:
            return render_template("curation/node_form.html", form=form)

        # Parse and normalize the nodes using the new implementation
        nodes, errors = parse_node_list(raw_nodes, client)

        # Show errors to user if any inputs couldn't be resolved
        if errors:
            flask.flash(f"Could not resolve the following inputs: {', '.join(errors)}")

        if not nodes:
            return render_template("curation/node_form.html", form=form)

        if len(nodes) > 30:
            flask.flash("Cannot query more than 30 nodes.")
            return render_template("curation/node_form.html", form=form)

        include_db_evidence = form.include_db_evidence.data
        # Redirect to the same route with query parameters
        # Format nodes as "prefix:identifier" strings for URL
        nodes_arg = ','.join(f"{prefix}:{identifier}" for prefix, identifier in nodes)
        return redirect(
            url_for('.subnetwork',
                    nodes=nodes_arg,
                    include_db_evidence=str(include_db_evidence).lower()))

    # If it's a GET request or form is not valid
    include_db_evidence = request.args.get('include_db_evidence',
                                           'true').lower() == 'true'
    nodes = [tuple(node.split(':', maxsplit=1))
             for node in request.args.get('nodes', '').split(',') if node]

    if nodes:
        # CLEAR OLD SESSION DATA BEFORE PROCESSING GET REQUEST
        session.pop('statement_hashes', None)
        session.pop('subnetwork_input_nodes', None)

        nodes_html = " ".join(
            f"""\
            <a class="badge badge-info" href="https://bioregistry.io/{prefix}:{identifier}" target="_blank">
                {prefix}:{identifier}
            </a>"""
            for prefix, identifier in nodes
        )

        stmts, source_counts = indra_subnetwork(
            nodes=nodes,
            client=client,
            include_db_evidence=include_db_evidence,
            order_by_ev_count=True,
            return_source_counts=True,
        )

        # EXTRACT AND STORE GENE NAMES (not IDs)
        # Get the actual gene names from statements
        input_gene_names = set()
        for stmt in stmts:
            for agent in stmt.agent_list():
                if agent and agent.name:
                    input_gene_names.add(agent.name)

        # Store the most frequent gene names (corresponding to input nodes)
        from collections import Counter
        # Count occurrences of each gene
        gene_counts = Counter()
        for stmt in stmts:
            for agent in stmt.agent_list():
                if agent and agent.name:
                    gene_counts[agent.name] += 1

        # Take top N genes where N = number of input nodes
        top_genes = {name for name, _ in gene_counts.most_common(len(nodes))}
        session['subnetwork_input_nodes'] = list(top_genes)
        logger.info(f"Stored {len(top_genes)} gene names in session: {top_genes}")

        return _enrich_render_statements(
            stmts,
            title="Subnetwork Explorer",
            description=f"""\
            The subnetwork explorer shows statements between the following nodes.
            {_database_text("Pathway Commons")}
            {EVIDENCE_TEXT}
            </p>
            <p>
            {nodes_html}
            """,
            no_stmts_message="No statements found for the given nodes.",
            include_db_evidence=include_db_evidence,
            source_counts=source_counts,
            store_hashes_in_session=True,
            prefix="subnetwork",
            identifier=','.join(f"{ns}:{id}" for ns, id in nodes)
        )

    # If no nodes provided, just render the form
    # Set the checkbox state based on URL parameter
    form.include_db_evidence.data = include_db_evidence
    return render_template("curation/node_form.html", form=form)


@explorer_blueprint.route("/api/get_network_from_hashes", methods=["POST"])
def get_network_from_hashes():
    """Get network visualization data from statement hashes."""
    logger.info("Starting get_network_from_hashes endpoint")

    # FIX 6: CHECK IF SESSION IS READY (for race condition)
    if 'statement_hashes' not in session:
        logger.warning("No statement_hashes in session - may be too early or session cleared")
        return jsonify({
            "nodes": [],
            "edges": [],
            "error": "Session not ready. Please wait for page to fully load."
        }), 202  # 202 = Accepted but not ready yet

    data = request.json
    include_db_evidence = data.get('include_db_evidence', True)

    logger.info("Generating network visualization using statements from session")
    network_data = get_network(
        include_db_evidence=include_db_evidence
    )

    if "error" in network_data:
        logger.warning(f"Error generating network: {network_data['error']}")
    else:
        logger.info(
            f"Network generated with {len(network_data.get('nodes', []))} nodes and {len(network_data.get('edges', []))} edges")

    return jsonify(network_data)


@explorer_blueprint.route("/statement/<int:stmt_hash>", methods=["GET"])
@jwt_required(optional=True)
def explore_statement(stmt_hash: int):
    """Explore all evidences for the statement."""
    include_db_evidence = request.args.get('include_db_evidence', 'false').lower() == 'true'
    start_time = time.time()
    enriched_stmts, evidence_counts = get_stmts_for_stmt_hashes(
        [stmt_hash],
        evidence_limit=10,
        return_evidence_counts=True,
        include_db_evidence=include_db_evidence
    )
    evidence_lookup_time = time.time() - start_time
    logger.info(f"Got statements in {evidence_lookup_time:.2f} seconds")
    return render_statements(
        enriched_stmts,
        title=f"Statement explorer: {stmt_hash}",
        evidence_counts=evidence_counts,
        evidence_lookup_time=evidence_lookup_time,
        description="Explore evidences from a single statement",
        include_db_evidence=include_db_evidence
    )
