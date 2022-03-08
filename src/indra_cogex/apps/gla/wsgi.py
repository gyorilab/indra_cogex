# -*- coding: utf-8 -*-

"""An app for gene list analysis."""

import os

from more_click import make_web_command

from .gene_blueprint import gene_blueprint
from .metabolite_blueprint import metabolite_blueprint
from .proxies import INDRA_COGEX_EXTENSION, client
from .. import get_flask_app
from ...client.neo4j_client import Neo4jClient

app = get_flask_app(__name__)

# Secret key must be set to use flask-wtf, but there's no *really*
# secure information in this app so it's okay to set randomly
app.config["WTF_CSRF_ENABLED"] = False
app.config["SECRET_KEY"] = os.urandom(8)

if not hasattr(app, "extensions"):
    print("no extensions?")
    app.extensions = {}

app.extensions[INDRA_COGEX_EXTENSION] = Neo4jClient()
app.register_blueprint(gene_blueprint)
app.register_blueprint(metabolite_blueprint)

cli = make_web_command(app=app)

if __name__ == "__main__":
    cli()
