# -*- coding: utf-8 -*-

"""Full INDRA CoGEx web app suite."""
import os
from pathlib import Path

from flask import Flask
from flask_bootstrap import Bootstrap4
from indralab_auth_tools.auth import auth, config_auth

from indra_cogex.apps.gla.gene_blueprint import gene_blueprint
from indra_cogex.apps.gla.metabolite_blueprint import metabolite_blueprint
from indra_cogex.apps.landing_page import landing_blueprint
from indra_cogex.apps.proxies import INDRA_COGEX_EXTENSION
from indra_cogex.apps.query_web_app import api
from indra_cogex.client.neo4j_client import Neo4jClient

APPS_DIR = Path(__file__).parent.absolute()
TEMPLATES_DIR = APPS_DIR / "templates"

app = Flask(__name__, template_folder=TEMPLATES_DIR)
app.register_blueprint(auth)
app.register_blueprint(landing_blueprint)
app.register_blueprint(gene_blueprint)
app.register_blueprint(metabolite_blueprint)
api.init_app(app)

if not hasattr(app, "extensions"):
    print("no extensions?")
    app.extensions = {}

app.extensions[INDRA_COGEX_EXTENSION] = Neo4jClient()

SC, jwt = config_auth(app)

# Secret key must be set to use flask-wtf, but there's no *really*
# secure information in this app so it's okay to set randomly
app.config["WTF_CSRF_ENABLED"] = False
app.config["SECRET_KEY"] = os.urandom(32)

bootstrap = Bootstrap4(app)

app.run()
