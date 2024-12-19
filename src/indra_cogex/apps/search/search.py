import json

from flask import Blueprint, render_template, request, jsonify, redirect, url_for
from flask_jwt_extended import jwt_required
from flask_wtf import FlaskForm
from indra.statements import get_all_descendants, Statement
from wtforms import StringField, SubmitField
from wtforms.fields.simple import BooleanField
from wtforms.validators import DataRequired

from indra_cogex.apps.utils import render_statements, format_stmts
from indra_cogex.client.queries import *
__all__ = ["search_blueprint"]

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


@search_blueprint.route("/", methods=['GET','POST'])
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

@search_blueprint.route("/gilda_ground", methods=["GET","POST"])
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