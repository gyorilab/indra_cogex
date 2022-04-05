"""
App serving different ways to display the return data from the various
CoGEx apps

Inspiration:
https://emmaa.indra.bio/evidence?model=covid19&source=model_statement&stmt_hash=26064355888834165&stmt_hash=-10390851375198442&stmt_hash=23381447023984348&stmt_hash=23140517349998103&stmt_hash=5866988047061824&stmt_hash=-35772916439665148&stmt_hash=3092749483304152&stmt_hash=-12269054015102995&stmt_hash=-26969793686025231&stmt_hash=4505325411022432&stmt_hash=-8847304092825154&stmt_hash=31213825008413749&stmt_hash=30141992178380666&stmt_hash=7575179353322188&stmt_hash=-30540958639363159&stmt_hash=31625126241403423&stmt_hash=26394120188094720&stmt_hash=-18911384093106728&stmt_hash=-12860666273291575&stmt_hash=-28802265300330434&stmt_hash=-2430129610717336&stmt_hash=1293890032279598&stmt_hash=3554404786891022&stmt_hash=-11125623700211639&stmt_hash=3687422032419285&stmt_hash=5305586621075360&stmt_hash=389677147118601&stmt_hash=4523113042432100&stmt_hash=-11643556207375872&stmt_hash=-9244908152322434&stmt_hash=11549424046359188&stmt_hash=29182661416104868&stmt_hash=-11508686685241868&stmt_hash=-27089380057920748&stmt_hash=-4984265834938630&stmt_hash=13171603590448017&stmt_hash=5248067513542319&stmt_hash=2633329737788340&stmt_hash=-17848096805003989&stmt_hash=-34885846099815193&stmt_hash=-16296155165635622&stmt_hash=12168088708840873&stmt_hash=29606940247996506&stmt_hash=20208230469852741&stmt_hash=-21459270342254616&stmt_hash=-21459270342254616&stmt_hash=11711788325523194&stmt_hash=-16093215807632509&stmt_hash=30007766770941473&stmt_hash=-1960362999519656
"""

import logging
from http import HTTPStatus
from os import environ
from typing import Union, Iterable

from flask import Blueprint, Response, abort, jsonify, render_template, request
from flask_jwt_extended import jwt_optional
from indra.sources.indra_db_rest import (
    IndraDBRestAPIError,
    get_curations,
    submit_curation,
)
from indralab_auth_tools.auth import resolve_auth

from indra_cogex.apps.proxies import client
from indra_cogex.apps.queries_web.helpers import process_result
from indra_cogex.client.queries import get_stmts_for_stmt_hashes

from ..utils import format_stmts

logger = logging.getLogger(__name__)

data_display_blueprint = Blueprint("data_display", __name__)

MORE_EVIDENCES_LIMIT = 10
LOCAL_VUE: Union[str, bool] = environ.get(
    "LOCAL_VUE", False
)  # Path to local vue dist


# Serve Vue components locally for testing
# If not testing, use the "latest" version (or some other deployment) of the Vue app from S3
if LOCAL_VUE:
    from flask import send_from_directory

    @data_display_blueprint.route("/vue/<path:file>", methods=["GET"])
    def serve_indralab_vue(file):
        return send_from_directory(LOCAL_VUE, file)

    VUE_SRC_JS = False
    VUE_SRC_CSS = False
else:
    vue_deployment = environ.get("VUE_DEPLOYMENT", "latest")
    vue_base = (
        f"https://bigmech.s3.amazonaws.com/indra-db/indralabvue-{vue_deployment}/"
    )
    VUE_SRC_JS = f"{vue_base}IndralabVue.umd.min.js"
    VUE_SRC_CSS = f"{vue_base}IndralabVue.css"

logger.info(f"Using Vue deployment at: {VUE_SRC_JS}")


# Endpoint for testing
@data_display_blueprint.route("/get_stmts", methods=["GET"])
def get_stmts():
    # Get the statements hash from the query string
    try:
        stmt_hash_list = request.args.getlist("stmt_hash", type=int)
        stmts = get_stmts_for_stmt_hashes(stmt_hash_list, client=client)
        return jsonify(process_result(stmts))
    except (TypeError, ValueError) as err:
        logger.exception(err)
        abort(Response("Parameter 'stmt_hash' unfilled", status=415))


# Serve the statement display template
@data_display_blueprint.route("/statement_display", methods=["GET"])
@jwt_optional
def statement_display():
    user, roles = resolve_auth(dict(request.args))
    email = user.email if user else ""

    # Get the statements hash from the query string
    try:
        stmt_hash_list = request.args.getlist("stmt_hash", type=int)
        if not stmt_hash_list:
            abort(
                Response(
                    "Parameter 'stmt_hash' unfilled", status=HTTPStatus.BAD_REQUEST
                )
            )
        relations: Iterable[Relation] = get_stmts_meta_for_stmt_hashes(
            stmt_hash_list, client=client
        )

        stmt_iter = [
            Statement._from_json(json.loads(r.data["stmt_json"])) for r in relations
        ]
        ev_counts = {r.data["stmt_hash"]: r.data["evidence_count"] for r in relations}
        stmts = format_stmts(stmts=stmt_iter, evidence_counts=ev_counts)
        return render_template(
            "data_display/data_display_base.html",
            stmts=stmts,
            user_email=email,
            vue_src_js=VUE_SRC_JS,
            vue_src_css=VUE_SRC_CSS,
        )
    except Exception as err:
        logger.exception(err)
        abort(status=HTTPStatus.INTERNAL_SERVER_ERROR)


def _get_user():
    user, roles = resolve_auth(dict(request.args))
    if not roles and not user:
        res_dict = {"result": "failure", "reason": "Invalid Credentials"}
        return jsonify(res_dict), 401

    if user:
        email = user.email
    else:
        email = request.json.get("email")
        if not email:
            res_dict = {
                "result": "failure",
                "reason": "POST with API key requires a user email.",
            }
            return jsonify(res_dict), 400

    return email


@data_display_blueprint.route("/curate/<hash_val>", methods=["POST"])
@jwt_optional
def submit_curation_endpoint(hash_val: str):
    email = _get_user()
    if not isinstance(email, str):
        return email

    logger.info("Adding curation for statement %s." % hash_val)
    ev_hash = request.json.get("ev_hash")
    source_api = request.json.pop("source", "CoGEx")
    tag = request.json.get("tag")
    text = request.json.get("text")
    is_test = "test" in request.args
    if not is_test:
        assert tag != "test"
        try:
            dbid = submit_curation(
                hash_val=int(hash_val),
                tag=tag,
                curator_email=email,
                text=text,
                ev_hash=ev_hash,
                source=source_api,
            )
        except IndraDBRestAPIError as e:
            abort(Response("Could not submit curation: %s." % e, 400))
        res = {"result": "success", "ref": {"id": dbid}}
    else:
        res = {"result": "test passed", "ref": None}
    logger.info("Got result: %s" % str(res))
    return jsonify(res)


@data_display_blueprint.route("/curation/list/<stmt_hash>/<src_hash>", methods=["GET"])
def list_curations(stmt_hash, src_hash):
    curations = get_curations(stmt_hash, source_hash=src_hash)
    return jsonify(curations)
