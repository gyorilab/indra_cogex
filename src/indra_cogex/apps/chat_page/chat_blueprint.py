"""Chat page app for INDRA CoGEx"""
import json

import flask
from flask import request

from indra_cogex.apps.constants import pusher_key, pusher_app

chat_blueprint = flask.Blueprint("chat", __name__, url_prefix="/chat")

__all__ = [
    "chat_blueprint",
]


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
