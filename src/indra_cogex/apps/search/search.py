import json
from typing import List, Optional, Mapping, Tuple

from flask import Blueprint, render_template, request, jsonify, redirect, url_for
from flask_jwt_extended import jwt_required
from flask_wtf import FlaskForm
from indra.statements import get_all_descendants, Statement
from wtforms import StringField, SubmitField
from wtforms.fields.simple import BooleanField
from wtforms.validators import DataRequired

from indra_cogex.apps.utils import render_statements
from indra_cogex.client import Neo4jClient, autoclient
from indra_cogex.client.queries import *
from indra_cogex.representation import norm_id

__all__ = ["search_blueprint"]

from indra_cogex.client.queries import enrich_statements

from indra_cogex.representation import indra_stmts_from_relations

search_blueprint = Blueprint("search", __name__, url_prefix="/search")


class SearchForm(FlaskForm):
    agent_name = StringField("Agent Name", validators=[DataRequired()])
    agent_role = StringField("Agent Role")
    other_agent = StringField("Other Agent")
    other_agent_role = StringField("Other Agent Role")
    source_type = StringField("Source Type")
    rel_type = StringField("Relationship Type")
    left_arrow = BooleanField("⇐")
    right_arrow = BooleanField("➔")
    both_arrow = BooleanField("⇔")
    paper_id = StringField("Paper ID")
    mesh_terms = StringField("MeSH Terms")
    submit = SubmitField("Search")


@search_blueprint.route("/", methods=['GET', 'POST'])
@jwt_required(optional=True)
def search():
    stmt_types = {c.__name__ for c in get_all_descendants(Statement)}
    stmt_types -= {"Influence", "Event", "Unresolved"}
    stmt_types_json = json.dumps(sorted(list(stmt_types)))

    form = SearchForm()

    # POST Request: Generate a sharable link with query parameters
    if form.validate_on_submit():
        query_params = {
            "agent": form.agent_name.data,
            "agent_tuple": request.form.get("agent_tuple"),
            "other_agent": form.other_agent.data,
            "other_agent_tuple": request.form.get("other_agent_tuple"),
            "source_type": form.source_type.data,
            "rel_types": json.loads(form.rel_type.data) if form.rel_type.data else None,
            "agent_role": form.agent_role.data,
            "other_role": form.other_agent_role.data,
            "paper_id": form.paper_id.data,
            "mesh_terms": form.mesh_terms.data,
            "mesh_tuple": request.form.get("mesh_tuple"),
        }
        query_params = {k: v for k, v in query_params.items() if v}
        return redirect(url_for("search.search", **query_params))

    # GET Request: Extract query parameters and fetch statements
    agent = request.args.get("agent")
    agent_tuple = request.args.get("agent_tuple")
    if agent_tuple:
        source_db, source_id = json.loads(agent_tuple)
        agent = (source_db, source_id)

    other_agent = request.args.get("other_agent")
    other_agent_tuple = request.args.get("other_agent_tuple")
    if other_agent_tuple:
        source_db, source_id = json.loads(other_agent_tuple)
        other_agent = (source_db, source_id)

    source_type = request.args.get("source_type")
    rel_types = request.args.getlist("rel_types")

    agent_role = request.args.get("agent_role")
    other_role = request.args.get("other_role")
    paper_id = request.args.get("paper_id")
    mesh_terms = request.args.get("mesh_terms")
    mesh_tuple = request.args.get("mesh_tuple")
    if mesh_tuple:
        source_db, source_id = json.loads(mesh_tuple)
        mesh_terms = (source_db, source_id)

    # Fetch and display statements
    if agent or other_agent or rel_types:
        statements, evidence_count = get_statements(
            agent=agent,
            agent_role=agent_role,
            other_agent=other_agent,
            other_role=other_role,
            stmt_sources=source_type,
            rel_types=rel_types,
            paper_term=paper_id,
            mesh_term=mesh_terms,
            limit=1000,
            minimum_evidence=1000,
            return_evidence_counts=True,
        )
        return render_statements(stmts=statements, evidence_count=evidence_count)

    # Render the form page
    return render_template(
        "search/search_page.html",
        form=form,
        stmt_types_json=stmt_types_json,
    )


from flask import current_app


@search_blueprint.route("/gilda_ground", methods=["GET", "POST"])
@jwt_required(optional=True)
def gilda_ground_endpoint():
    data = request.get_json()
    current_app.logger.info(f"Received payload: {data}")
    agent_text = data.get("agent")
    if not agent_text:
        return {"error": "Agent text is required."}, 400

    try:
        gilda_list = gilda_ground(agent_text)
        return jsonify(gilda_list)
    except Exception as e:
        return {"error": str(e)}, 500


def gilda_ground(agent_text):
    try:
        from gilda.api import ground
        return [r.to_json() for r in ground(agent_text)]
    except ImportError:
        import requests
        res = requests.post('http://grounding.indra.bio/ground', json={'text': agent_text})
        return res.json()
    except Exception as e:
        return {"error": f"Grounding failed: {str(e)}"}


@autoclient()
def get_ora_statements(
    target_id: str,
    genes: List[str],
    minimum_belief: float = 0.0,
    minimum_evidence: Optional[int] = None,
    is_downstream: bool = False,
    *,
    client: Neo4jClient,
) -> Tuple[List[Statement], Mapping[int, int]]:
    """Get statements connecting input genes to target entity for ORA analysis.

    Parameters
    ----------
    target_id : str
        The ID of the target entity (e.g., 'GO:0006955', 'MESH:D007239')
    genes : List[str]
        List of gene IDs (e.g., ['HGNC:6019', 'HGNC:11876'])
    minimum_belief : float
        Minimum belief score for relationships
    minimum_evidence : Optional[int]
        Minimum number of evidences required for a statement to be included
    is_downstream : bool
        Whether this is a downstream analysis
    client : Neo4jClient
        The Neo4j client to use for querying

    Returns
    -------
    :
        A tuple containing:
        - List of INDRA statements representing the relationships
        - Dictionary mapping statement hashes to their evidence counts
    """
    # Normalize gene IDs
    normalized_genes = [norm_id('HGNC', gene.split(':')[1]) for gene in genes]
    print(f"DEBUG: Normalized genes: {normalized_genes[:5]}...")

    # Handle different entity types and their relationships
    namespace = target_id.split(':')[0].lower()
    id_part = target_id.split(':')[1]

    if namespace == 'mesh':
        normalized_target = f"mesh:{id_part}"
        rel_types = ["indra_rel", "has_indication"]
    elif namespace == 'fplx':
        normalized_target = f"fplx:{id_part}"
        rel_types = ["indra_rel", "isa"]
    else:
        normalized_target = target_id.lower()
        rel_types = ["indra_rel"]

    # Main query for getting statements
    query = """
    MATCH p = (d:BioEntity {id: $target_id})-[r]->(u:BioEntity)
    WHERE type(r) IN $rel_types
    AND u.id STARTS WITH "hgnc"
    AND NOT u.obsolete
    AND u.id IN $genes
    AND (type(r) <> 'indra_rel' OR r.belief > $minimum_belief)
    WITH distinct r.stmt_hash AS hash, collect(p) as pp
    RETURN pp
    """

    if is_downstream:
        query = """
        MATCH p = (u:BioEntity)-[r]->(d:BioEntity {id: $target_id})
        WHERE type(r) IN $rel_types
        AND u.id STARTS WITH "hgnc"
        AND NOT u.obsolete
        AND u.id IN $genes
        AND (type(r) <> 'indra_rel' OR r.belief > $minimum_belief)
        WITH distinct r.stmt_hash AS hash, collect(p) as pp
        RETURN pp
        """

    params = {
        "target_id": normalized_target,
        "genes": normalized_genes,
        "rel_types": rel_types,
        "minimum_belief": minimum_belief
    }
    results = client.query_tx(query, **params)
    flattened_rels = [client.neo4j_to_relation(i[0]) for rel in results for i in rel]

    # Filter relations based on minimum_evidence
    if minimum_evidence:
        flattened_rels = [
            rel for rel in flattened_rels
            if rel.data.get("evidence_count", 0) >= minimum_evidence
        ]

    stmts = indra_stmts_from_relations(flattened_rels, deduplicate=True)

    # Enrich statements with complete evidence (no limit)
    stmts = enrich_statements(
            stmts,
            client=client
    )

    # Create evidence count mapping
    evidence_counts = {
            stmt.get_hash(): rel.data.get("evidence_count", 0)
            for rel, stmt in zip(flattened_rels, stmts)
    }

    return stmts, evidence_counts


@search_blueprint.route("/ora_statements/", methods=['GET'])
@jwt_required(optional=True)
def search_ora_statements():
    """Endpoint to get INDRA statements connecting input genes to a target entity."""
    target_id = request.args.get("target_id")
    genes = request.args.getlist("genes")
    is_downstream = request.args.get("is_downstream", "").lower() == "true"

    try:
        minimum_evidence = int(request.args.get("minimum_evidence") or 2)
        minimum_belief = float(request.args.get("minimum_belief") or 0.0)
    except (ValueError, TypeError):
        return jsonify({"error": "Invalid parameter values"}), 400

    if not target_id or not genes:
        return jsonify({"error": "target_id and genes are required"}), 400

    try:
        statements, evidence_counts = get_ora_statements(
            target_id=target_id,
            genes=genes,
            minimum_belief=minimum_belief,
            minimum_evidence=minimum_evidence,
            is_downstream=is_downstream
        )

        return render_statements(
            stmts=statements,
            evidence_count=evidence_counts
        )

    except Exception as e:
        print(f"Error in get_ora_statements: {str(e)}")
        return jsonify({"error": str(e)}), 500
