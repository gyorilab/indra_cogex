import json
from typing import List, Optional, Mapping, Tuple

from flask import Blueprint, render_template, request, jsonify, redirect, url_for
from flask_jwt_extended import jwt_required
from flask_wtf import FlaskForm
from indra.statements import get_all_descendants, Statement
from wtforms import StringField, SubmitField
from wtforms.fields.simple import BooleanField
from wtforms.validators import DataRequired

from indra_cogex.apps.utils import render_statements, format_stmts
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
            evidence_limit=1000,
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
    evidence_limit: Optional[int] = None,
    *,
    client: Neo4jClient,
) -> Tuple[List[Statement], Mapping[int, int]]:
    """Get statements connecting input genes to target entity for ORA analysis.

    Parameters
    ----------
    target_id : str
        The ID of the target entity (e.g., 'GO:0006955')
    genes : List[str]
        List of gene IDs (e.g., ['HGNC:6019', 'HGNC:11876'])
    minimum_belief : float
        Minimum belief score for relationships
    evidence_limit : Optional[int]
        Maximum number of evidence entries per statement
    client : Neo4jClient
        The Neo4j client to use for querying

    Returns
    -------
    :
        A tuple containing:
        - List of INDRA statements representing the relationships
        - Dictionary mapping statement hashes to their evidence counts
    """
    print("\nDEBUG: Starting get_ora_statements")
    print(f"Original inputs - target_id: {target_id}, genes: {genes}")

    # Normalize IDs
    normalized_target = target_id.lower()
    normalized_genes = [norm_id('HGNC', gene.split(':')[1]) for gene in genes]

    print(f"Normalized IDs - target: {normalized_target}")
    print(f"Normalized genes: {normalized_genes[:5]}... (showing first 5)")

    # Check if target exists
    verify_query = """
   MATCH (d:BioEntity {id: $target_id})
   RETURN d
   """
    target_result = client.query_tx(verify_query, target_id=normalized_target)
    print(f"\nDEBUG: Target node exists?: {bool(target_result)}")

    # Main query using filters from get_entity_to_targets/regulators
    query = """
   MATCH (d:BioEntity {id: $target_id})<-[r:indra_rel]-(u:BioEntity)
   WHERE u.id STARTS WITH "hgnc"
   AND NOT u.obsolete
   AND r.stmt_type <> "Complex"
   AND u.id IN $genes 
   AND r.belief > $minimum_belief
   RETURN r, u
   """

    params = {
        "target_id": normalized_target,
        "genes": normalized_genes,
        "minimum_belief": minimum_belief
    }

    print("\nDEBUG: Executing main query:")
    print(f"Query: {query}")
    print(f"Parameters: {params}")

    results = client.query_tx(query, **params)
    print(f"\nDEBUG: Raw query results length: {len(results)}")

    if results:
        print(f"Sample result: {results[0]}")  # Show first result if any

    flattened_rels = [client.neo4j_to_relation(i[0]) for rel in results for i in rel]
    print(f"\nDEBUG: Number of flattened relations: {len(flattened_rels)}")

    stmts = indra_stmts_from_relations(flattened_rels, deduplicate=True)
    print(f"\nDEBUG: Number of statements before enrichment: {len(stmts)}")

    if evidence_limit:
        stmts = enrich_statements(
            stmts,
            client=client,
            evidence_limit=evidence_limit,
        )
        print(f"DEBUG: Number of statements after enrichment: {len(stmts)}")

    evidence_counts = {
        stmt.get_hash(): (
            min(rel.data["evidence_count"], evidence_limit)
            if evidence_limit is not None
            else rel.data["evidence_count"]
        )
        for rel, stmt in zip(flattened_rels, stmts)
    }

    print(f"\nDEBUG: Final output - Number of statements: {len(stmts)}")
    print(f"DEBUG: Evidence counts: {evidence_counts}")

    return stmts, evidence_counts


@search_blueprint.route("/ora_statements/", methods=['GET'])
@jwt_required(optional=True)
def search_ora_statements():
    """Endpoint to get INDRA statements connecting input genes to a target entity."""
    target_id = request.args.get("target_id")
    genes = request.args.getlist("genes")

    # Add logging
    print("Received parameters:")
    print(f"target_id: {target_id}")
    print(f"genes: {genes}")
    print(f"minimum_evidence: {request.args.get('minimum_evidence')}")
    print(f"minimum_belief: {request.args.get('minimum_belief')}")

    try:
        evidence_limit = int(request.args.get("minimum_evidence") or 2)
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
            evidence_limit=evidence_limit
        )

        # Add logging
        print(f"Number of statements found: {len(statements)}")

        return render_statements(
            stmts=statements,
            evidence_count=evidence_counts
        )

    except Exception as e:
        print(f"Error in get_ora_statements: {str(e)}")  # Add error logging
        return jsonify({"error": str(e)}), 500
