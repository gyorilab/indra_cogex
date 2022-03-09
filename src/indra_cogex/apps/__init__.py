# -*- coding: utf-8 -*-

"""Apps built on top of INDRA CoGEx."""

from pathlib import Path
from flask import Flask

APPS_DIR = Path(__file__).parent.absolute()
TEMPLATES_DIR = APPS_DIR / 'templates'


def get_flask_app(app_name: str) -> Flask:
    """Return a Flask app initialized with login blueprint

    Parameters
    ----------
    app_name :
        Name of the app.

    Returns
    -------
    :
        Flask app.
    """
    from flask_bootstrap import Bootstrap4
    from indralab_auth_tools.auth import auth, config_auth
    from indra_cogex.apps.proxies import INDRA_COGEX_EXTENSION
    from ..client.neo4j_client import Neo4jClient

    app = Flask(app_name, template_folder=TEMPLATES_DIR)
    app.register_blueprint(auth)
    if not hasattr(app, "extensions"):
        print("no extensions?")
        app.extensions = {}

    app.extensions[INDRA_COGEX_EXTENSION] = Neo4jClient()

    SC, jwt = config_auth(app)
    bootstrap = Bootstrap4(app)
    return app
