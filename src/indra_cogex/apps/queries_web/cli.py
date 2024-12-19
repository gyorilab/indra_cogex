from flask import Flask
from more_click import make_web_command

from indra_cogex.apps.constants import INDRA_COGEX_EXTENSION
from indra_cogex.apps.rest_api import api
from indra_cogex.client import Neo4jClient

app = Flask(__name__)
api.init_app(app)
app.extensions[INDRA_COGEX_EXTENSION] = Neo4jClient()
cli = make_web_command(app=app)

if __name__ == "__main__":
    cli()
