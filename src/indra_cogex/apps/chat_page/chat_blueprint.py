"""Chat page app for INDRA CoGEx"""
import json
import logging
from pathlib import Path

import flask
from flask import request

from indra_cogex.apps.constants import pusher_key, pusher_app, LOCAL_VUE, STATIC_DIR

logger = logging.getLogger(__name__)

chat_blueprint = flask.Blueprint("chat", __name__, url_prefix="/chat")

__all__ = [
    "chat_blueprint",
]

# Serve vue app locally for testing
if LOCAL_VUE:
    from flask import send_from_directory

    if (isinstance(LOCAL_VUE, str) and not Path(LOCAL_VUE).is_dir()) or isinstance(LOCAL_VUE, bool):
        DIST = STATIC_DIR / "vue-chat" / "dist"
    else:
        DIST = Path(LOCAL_VUE)

    logger.info(f"Serving vue app locally from {DIST}")

    @chat_blueprint.route("/vue/<path:file>")
    def serve_vue(file):
        return send_from_directory(DIST.absolute().as_posix(), file)

else:
    logger.info("Serving vue app from [not implemented]")


@chat_blueprint.route("/")
def chat_page():
    """Chat page"""
    return flask.render_template(
        "chat/chat_page.html",
        pusher_app_key=pusher_key,
    )


@chat_blueprint.route("/new/guest", methods=["POST"])
def guestUser():
    data = request.json

    pusher_app.trigger(
        u"general-channel",
        u"new-guest-details",
        {"name": data["name"], "email": data["email"]},
    )

    return json.dumps(data)


@chat_blueprint.route("/pusher/auth", methods=["POST"])
def pusher_authentication():
    auth = pusher_app.authenticate(
        channel=request.form["channel_name"], socket_id=request.form["socket_id"]
    )
    return json.dumps(auth)
