from flask import Flask
from more_click import make_web_command

from indra_cogex.apps.query_web_app import api

app = Flask(__name__)
api.init_app(app)
cli = make_web_command(app=app)

if __name__ == "__main__":
    cli()
