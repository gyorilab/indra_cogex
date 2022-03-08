import logging

from flask import render_template
from more_click import make_web_command

from .. import get_flask_app

logger = logging.getLogger(__name__)

app = get_flask_app(__name__)


@app.route("/")
def landing_page():
    return render_template("landing_page.html")


# Create runnable cli command
cli = make_web_command(app)

if __name__ == "__main__":
    cli()
