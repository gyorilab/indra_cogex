import json

from flask import Blueprint, render_template, request, jsonify
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

    if form.validate_on_submit():
        agent = form.agent_name.data
        agent_tuple = request.form.get('agent_tuple')  # This is a JSON string
        if agent_tuple:
            source_db, source_id = json.loads(agent_tuple)
            agent = (source_db,source_id)
        other_agent = form.other_agent.data
        source_type = form.source_type.data
        if form.rel_type.data:
            rel_types = json.loads(form.rel_type.data)
        else: rel_types = None
        agent_role = form.agent_role.data
        other_role = form.other_agent_role.data
        paper_id = form.paper_id.data
        mesh_terms = form.mesh_terms.data
        mesh_tuple = request.form.get('mesh_tuple')
        if mesh_tuple:
            source_db, source_id = json.loads(mesh_tuple)
            mesh_terms = (source_db, source_id)

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
            evidence_limit=2000,
            return_evidence_counts=True
        )
        return render_statements(stmts=statements, evidence_count=evidence_count)

    return render_template("search/search_page.html",
                           form=form,
                           stmt_types_json=stmt_types_json)
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