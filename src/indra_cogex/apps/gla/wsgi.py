# -*- coding: utf-8 -*-

"""An app for gene list analysis."""

import os

from flask import Flask
from flask_bootstrap import Bootstrap4
from indralab_auth_tools.auth import auth, config_auth
from more_click import make_web_command

from indra_cogex.apps.constants import INDRA_COGEX_EXTENSION, STATIC_DIR, TEMPLATES_DIR

from .gene_blueprint import gene_blueprint
from .metabolite_blueprint import metabolite_blueprint
from indra_cogex.apps.gla.source_target_blueprint import source_target_blueprint
from ...client.neo4j_client import Neo4jClient

app = Flask(__name__, template_folder=str(TEMPLATES_DIR), static_folder=STATIC_DIR)

bootstrap = Bootstrap4(app)

app.extensions[INDRA_COGEX_EXTENSION] = Neo4jClient()

SC, jwt = config_auth(app)

# Secret key must be set to use flask-wtf, but there's no *really*
# secure information in this app so it's okay to set randomly
app.config["WTF_CSRF_ENABLED"] = False
app.config["SECRET_KEY"] = os.urandom(8)

# Register blueprints
print("Registering blueprints")
app.register_blueprint(auth)
print("Auth registered")
app.register_blueprint(gene_blueprint)
print("Gene registered")
app.register_blueprint(metabolite_blueprint)
print("Metabolite registered")
app.register_blueprint(source_target_blueprint)
print("Source Target registered")


def list_routes():
    print("\nAll registered routes:")
    for rule in app.url_map.iter_rules():
        print(f"{rule.endpoint} -> {rule.rule}")


list_routes()

cli = make_web_command(app=app)

if __name__ == "__main__":
    cli()
