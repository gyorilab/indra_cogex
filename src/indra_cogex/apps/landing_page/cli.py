import flask
from flask_bootstrap import Bootstrap4
from indralab_auth_tools.auth import auth, config_auth
from more_click import make_web_command

from indra_cogex.apps import STATIC_DIR, TEMPLATES_DIR
from indra_cogex.apps.landing_page import landing_blueprint
from indra_cogex.apps.proxies import INDRA_COGEX_EXTENSION
from indra_cogex.client import Neo4jClient

app = flask.Flask(__name__, template_folder=TEMPLATES_DIR, static_folder=STATIC_DIR)
app.register_blueprint(auth)
app.register_blueprint(landing_blueprint)

if not hasattr(app, "extensions"):
    print("no extensions?")
    app.extensions = {}

app.extensions[INDRA_COGEX_EXTENSION] = Neo4jClient()
SC, jwt = config_auth(app)
cli = make_web_command(app)
bootstrap = Bootstrap4(app)

if __name__ == "__main__":
    cli()
