# -*- coding: utf-8 -*-

"""An app for gene list analysis."""

import os
from pathlib import Path

from flask import Flask
from flask_bootstrap import Bootstrap4
from indralab_auth_tools.auth import auth, config_auth
from more_click import make_web_command

from indra_cogex.apps.proxies import INDRA_COGEX_EXTENSION

from .gene_blueprint import gene_blueprint
from .metabolite_blueprint import metabolite_blueprint
from ...client.neo4j_client import Neo4jClient

APPS_DIR = Path(__file__).parent.parent.absolute()
TEMPLATES_DIR = APPS_DIR / "templates"
STATIC_DIR = APPS_DIR / "static"

app = Flask(__name__, template_folder=str(TEMPLATES_DIR), static_folder=STATIC_DIR)

if not hasattr(app, "extensions"):
    print("no extensions?")
    app.extensions = {}

bootstrap = Bootstrap4(app)

app.extensions[INDRA_COGEX_EXTENSION] = Neo4jClient()

SC, jwt = config_auth(app)

# Secret key must be set to use flask-wtf, but there's no *really*
# secure information in this app so it's okay to set randomly
app.config["WTF_CSRF_ENABLED"] = False
app.config["SECRET_KEY"] = os.urandom(8)

# Register blueprints
app.register_blueprint(auth)
app.register_blueprint(gene_blueprint)
app.register_blueprint(metabolite_blueprint)

cli = make_web_command(app=app)

if __name__ == "__main__":
    cli()
