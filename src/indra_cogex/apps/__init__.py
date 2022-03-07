# -*- coding: utf-8 -*-

"""Apps built on top of INDRA CoGEx."""

from pathlib import Path
from flask import Flask

APPS_DIR = Path(__file__).parent.absolute()
TEMPLATES_DIR = APPS_DIR / 'templates'


def get_flask_app(app_name) -> Flask:
    """Return a Flask app."""
    from flask import Flask
    from indralab_auth_tools.auth import auth, config_auth
    app = Flask(app_name, template_folder=TEMPLATES_DIR)
    app.register_blueprint(auth)
    SC, jwt = config_auth(app)
    return app
