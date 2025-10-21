# -*- coding: utf-8 -*-

"""Full INDRA CoGEx web app suite."""

import logging
import os
from pathlib import Path

from flask import Flask
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
from indra_cogex.client.enrichment.utils import build_caches
from indra_cogex.apps.search import search_blueprint

logger = logging.getLogger(__name__)


app = Flask(__name__, template_folder=TEMPLATES_DIR, static_folder=STATIC_DIR)

#AUTO-CREATE SESSION DIRECTORY (No manual bash commands needed!)
SESSION_DIR = '/tmp/flask_session'
Path(SESSION_DIR).mkdir(parents=True, exist_ok=True)
logger.info(f"Session directory created/verified at: {SESSION_DIR}")

#SERVER-SIDE SESSION CONFIGURATION
app.config['SESSION_TYPE'] = 'filesystem'
app.config['SESSION_FILE_DIR'] = SESSION_DIR
app.config['SESSION_PERMANENT'] = False
app.config['SESSION_USE_SIGNER'] = True

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

# Secret key must be set to use flask-wtf, but there's no *really*
# secure information in this app so it's okay to set randomly
app.config["WTF_CSRF_ENABLED"] = False
app.config["SECRET_KEY"] = os.urandom(32)
app.config["SWAGGER_UI_DOC_EXPANSION"] = "list"
app.config["EXPLAIN_TEMPLATE_LOADING"] = False

# INITIALIZE SERVER-SIDE SESSIONS (After SECRET_KEY is set)
Session(app)
logger.info("Server-side sessions initialized")

bootstrap = Bootstrap4(app)

build_caches()