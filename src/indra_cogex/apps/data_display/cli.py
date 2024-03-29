from flask import Flask
from flask_bootstrap import Bootstrap4
from indra_cogex.apps.curation_cache import CurationCache
from indralab_auth_tools.auth import auth, config_auth
from more_click import make_web_command

from indra_cogex.apps.constants import (
    INDRA_COGEX_EXTENSION,
    STATIC_DIR,
    TEMPLATES_DIR,
    STATEMENT_CURATION_CACHE
)
from indra_cogex.apps.data_display import data_display_blueprint
from indra_cogex.client.neo4j_client import Neo4jClient

app = Flask(__name__, template_folder=TEMPLATES_DIR, static_folder=STATIC_DIR)
bootstrap = Bootstrap4(app)
app.extensions[INDRA_COGEX_EXTENSION] = Neo4jClient()
app.extensions[STATEMENT_CURATION_CACHE] = CurationCache()
app.register_blueprint(auth)
app.register_blueprint(data_display_blueprint)
SC, jwt = config_auth(app)
cli = make_web_command(app)

if __name__ == "__main__":
    cli()
