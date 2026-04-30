# -*- coding: utf-8 -*-

"""Full INDRA CoGEx web app suite."""

import logging
import os
from pathlib import Path

from flask import Flask, url_for as flask_url_for
from flask_bootstrap import Bootstrap4
from flask_session import Session
from indralab_auth_tools.auth import auth, config_auth

from indra_cogex.apps.constants import (
    INDRA_COGEX_EXTENSION,
    STATIC_DIR,
    TEMPLATES_DIR,
    STATEMENT_CURATION_CACHE,
    AGENT_NAME_CACHE,
)
from indra_cogex.apps.chat_page import chat_blueprint
from indra_cogex.apps.curator import explorer_blueprint
from indra_cogex.apps.curation_cache import CurationCache
from indra_cogex.apps.data_display import data_display_blueprint
from indra_cogex.apps.gla.gene_blueprint import gene_blueprint
from indra_cogex.apps.gla.metabolite_blueprint import metabolite_blueprint
from indra_cogex.apps.gla.source_target_blueprint import source_target_blueprint
from indra_cogex.apps.home import home_blueprint
from indra_cogex.apps.rest_api import api
from indra_cogex.client.neo4j_client import Neo4jClient
from indra_cogex.apps.search import search_blueprint

logger = logging.getLogger(__name__)


ROOT_PATH = os.environ.get("DISCOVERY_ROOT_PATH")


def url_for(endpoint, **values):
    """Custom url_for to handle ROOT_PATH if set."""
    if ROOT_PATH:
        normalized_root = ROOT_PATH.rstrip("/")
        with_values = flask_url_for(endpoint, **values)
        if with_values == normalized_root or with_values.startswith(f"{normalized_root}/"):
            return with_values
        return normalized_root + with_values
    return flask_url_for(endpoint, **values)

app = Flask(__name__, template_folder=TEMPLATES_DIR, static_folder=STATIC_DIR)
app.jinja_env.globals['url_for'] = url_for

# AUTO-CREATE SESSION DIRECTORY (No manual bash commands needed!)
SESSION_DIR = '/tmp/flask_session'
Path(SESSION_DIR).mkdir(parents=True, exist_ok=True)
logger.info(f"Session directory created/verified at: {SESSION_DIR}")

# SERVER-SIDE SESSION CONFIGURATION
app.config['SESSION_TYPE'] = 'filesystem'
app.config['SESSION_FILE_DIR'] = SESSION_DIR
app.config['SESSION_PERMANENT'] = False
app.config['SESSION_USE_SIGNER'] = True
if ROOT_PATH:
    app.config["APPLICATION_ROOT"] = ROOT_PATH.rstrip("/")
    app.config['SESSION_COOKIE_PATH'] = "/"
    root_prefix = app.config["APPLICATION_ROOT"]

    class ScriptNamePrefixMiddleware:
        """Inject SCRIPT_NAME before Flask binds request URL adapter."""

        def __init__(self, wsgi_app, script_name):
            self.wsgi_app = wsgi_app
            self.script_name = script_name

        def __call__(self, environ, start_response):
            environ["SCRIPT_NAME"] = self.script_name
            return self.wsgi_app(environ, start_response)

    app.wsgi_app = ScriptNamePrefixMiddleware(app.wsgi_app, root_prefix)

# Register blueprints (already there)
app.register_blueprint(auth)
app.register_blueprint(home_blueprint)
app.register_blueprint(gene_blueprint)
app.register_blueprint(metabolite_blueprint)
app.register_blueprint(data_display_blueprint)
app.register_blueprint(explorer_blueprint)
app.register_blueprint(chat_blueprint)
app.register_blueprint(search_blueprint)
app.register_blueprint(source_target_blueprint)
api.init_app(app)

app.extensions[INDRA_COGEX_EXTENSION] = Neo4jClient()
app.extensions[STATEMENT_CURATION_CACHE] = CurationCache()

config_auth(app)

# Secret key must be stable across production workers.
app.config["WTF_CSRF_ENABLED"] = False
app.config["SECRET_KEY"] = os.environ.get("FLASK_SECRET_KEY") or os.urandom(32)
app.config["SWAGGER_UI_DOC_EXPANSION"] = "list"
app.config["EXPLAIN_TEMPLATE_LOADING"] = False

# INITIALIZE SERVER-SIDE SESSIONS (After SECRET_KEY is set)
Session(app)
logger.info("Server-side sessions initialized")

bootstrap = Bootstrap4(app)
