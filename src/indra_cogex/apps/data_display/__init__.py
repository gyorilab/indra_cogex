"""
App serving different ways to display the return data from the various
CoGEx apps

Inspiration:
https://emmaa.indra.bio/evidence?model=covid19&source=model_statement&stmt_hash=26064355888834165&stmt_hash=-10390851375198442&stmt_hash=23381447023984348&stmt_hash=23140517349998103&stmt_hash=5866988047061824&stmt_hash=-35772916439665148&stmt_hash=3092749483304152&stmt_hash=-12269054015102995&stmt_hash=-26969793686025231&stmt_hash=4505325411022432&stmt_hash=-8847304092825154&stmt_hash=31213825008413749&stmt_hash=30141992178380666&stmt_hash=7575179353322188&stmt_hash=-30540958639363159&stmt_hash=31625126241403423&stmt_hash=26394120188094720&stmt_hash=-18911384093106728&stmt_hash=-12860666273291575&stmt_hash=-28802265300330434&stmt_hash=-2430129610717336&stmt_hash=1293890032279598&stmt_hash=3554404786891022&stmt_hash=-11125623700211639&stmt_hash=3687422032419285&stmt_hash=5305586621075360&stmt_hash=389677147118601&stmt_hash=4523113042432100&stmt_hash=-11643556207375872&stmt_hash=-9244908152322434&stmt_hash=11549424046359188&stmt_hash=29182661416104868&stmt_hash=-11508686685241868&stmt_hash=-27089380057920748&stmt_hash=-4984265834938630&stmt_hash=13171603590448017&stmt_hash=5248067513542319&stmt_hash=2633329737788340&stmt_hash=-17848096805003989&stmt_hash=-34885846099815193&stmt_hash=-16296155165635622&stmt_hash=12168088708840873&stmt_hash=29606940247996506&stmt_hash=20208230469852741&stmt_hash=-21459270342254616&stmt_hash=-21459270342254616&stmt_hash=11711788325523194&stmt_hash=-16093215807632509&stmt_hash=30007766770941473&stmt_hash=-1960362999519656
"""
import json
import logging
from http import HTTPStatus
from os import environ
from typing import Union, Iterable, Any, Dict, List, Optional, Set

from flask import Blueprint, Response, abort, jsonify, render_template, request
from flask_jwt_extended import jwt_optional
from indra.sources.indra_db_rest import (
    IndraDBRestAPIError,
    get_curations,
    submit_curation,
)
from indra.statements import Statement
from indralab_auth_tools.auth import resolve_auth

from indra_cogex.apps.proxies import client
from indra_cogex.apps.queries_web.helpers import process_result
from indra_cogex.client.queries import (
    get_stmts_for_stmt_hashes,
    get_evidences_for_stmt_hash,
    get_stmts_meta_for_stmt_hashes,
)

from ..utils import format_stmts
from ...representation import Relation

logger = logging.getLogger(__name__)

data_display_blueprint = Blueprint("data_display", __name__)

MORE_EVIDENCES_LIMIT = 10
LOCAL_VUE: Union[str, bool] = environ.get("LOCAL_VUE", False)  # Path to local vue dist


# Get curations stores globally for now
curations = get_curations()

curations_by_stmt_hash: Dict[int, Dict[int, List[Dict[str, any]]]] = {}
for c in curations:
    if c["pa_hash"] not in curations_by_stmt_hash:
        curations_by_stmt_hash[c["pa_hash"]] = {}
    if c["source_hash"] not in curations_by_stmt_hash[c["pa_hash"]]:
        curations_by_stmt_hash[c["pa_hash"]][c["source_hash"]] = []

    curations_by_stmt_hash[c["pa_hash"]][c["source_hash"]].append(c)


def format_ev_json(
    ev_list: Iterable[Dict[str, Any]],
    curations_for_stmt: Dict[int, List[Dict[str, any]]],
    correct_tags: Optional[Set[str]] = None,
) -> List[Dict[str, Any]]:
    """Format the evidence json for display by the Vue component

    Parameters
    ----------
    ev_list :
        An iterable of evidence jsons to be formatted
    curations_for_stmt :
        A dict of curations for the stmt_hash
    correct_tags :
        A tuple of tags to be considered correct

    Returns
    -------
    :
        The formatted evidence jsons
    """
    correct_tags = correct_tags or {"correct", "hypothesis", "act_vs_amt"}
    # The <Evidence> vue component expects this json structure:
    #     props: {
    #       text: String,
    #       pmid: String,
    #       source_api: String,
    #       text_refs: Object,
    #       num_curations: Number,
    #       num_correct: {
    #         type: Number,
    #         default: null
    #       },
    #       num_incorrect: {
    #         type: Number,
    #         default: null
    #       },
    #       source_hash: {
    #         type: String,
    #         required: true
    #       },
    #       original_json: Object,
    #     },
    fmtd_ev_jsons = []
    for ev_json in ev_list:
        src_hash = ev_json["source_hash"]
        src_hash_str = str(src_hash)
        ev_json["source_hash"] = src_hash_str

        # Number of curations for this evidence
        num_curations = len(curations_for_stmt.get(src_hash, []))

        # Number of "correct" curations for this evidence
        num_correct = len(
            [
                c
                for c in curations_for_stmt.get(src_hash, [])
                if c["tag"] in correct_tags
            ]
        )

        # Number of "incorrect" curations for this evidence
        num_incorrect = num_curations - num_correct

        fmt_json = {
            "text": ev_json["text"],
            "pmid": ev_json.get("pmid", ""),
            "source_api": ev_json["source_api"],
            "text_refs": ev_json["text_refs"],
            "source_hash": src_hash_str,
            "original_json": ev_json,
            "num_curations": num_curations,
            "num_correct": num_correct,
            "num_incorrect": num_incorrect,
        }
        fmtd_ev_jsons.append(fmt_json)
    return fmtd_ev_jsons


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


# Endpoint for getting evidence
@data_display_blueprint.route("/expand/<stmt_hash>", methods=["GET"])
@jwt_optional
def get_evidence(stmt_hash):
    try:
        # Todo:
        #  1. ideally, only the call to fetch evidences should be needed,
        #     but for that to work, format_stmts() needs to be refactored
        #     and the part that goes through the evidences should be
        #     refactored out
        #  2. The call to get curations is taking most of the time, it would
        #     be good to cache the curations for the stmt_hash and only
        #     fetch them at a regular interval or when the user requests it

        # Check if user is authenticated to allow medscan evidence
        user, roles = resolve_auth(dict(request.args))

        # If user is None, remove medscan
        remove_medscan = user is None

        # limit = request.args.get("limit", type=int, default=MORE_EVIDENCES_LIMIT)
        limit = request.args.get("limit", type=int)
        offset = request.args.get("offset", type=int, default=0)
        ev_objs = get_evidences_for_stmt_hash(
            stmt_hash=stmt_hash,
            client=client,
            limit=limit,
            offset=offset,  # Sets value for SKIP
            remove_medscan=remove_medscan,
        )

        # <Statement> expects this json structure:
        # resp_json.statements[hash].evidence
        # Format evidence json: stmt_hash and source_hash need to be strings

        # Get the relation for this statement
        relations: Iterable[Relation] = get_stmts_meta_for_stmt_hashes(
            stmt_hashes=[stmt_hash],
            client=client,
        )

        # Get the statement and then extend the
        stmt_iter = [
            Statement._from_json(json.loads(r.data["stmt_json"])) for r in relations
        ]
        stmt: Statement = stmt_iter[0]
        stmt.evidence += [ev for ev in ev_objs if not ev.equals(stmt.evidence[0])]

        # Get the evidence counts
        ev_counts = {r.data["stmt_hash"]: r.data["evidence_count"] for r in relations}

        # Get the formatted evidence rows
        stmt_rows = format_stmts(stmts=stmt_iter, evidence_counts=ev_counts)

        # Return the evidence json for the statement

        # <Statement> expects this json structure:
        # resp_json.statements[hash].evidence
        # Note that 'stmt_hash' and 'source_hash' need to be strings
        return jsonify(
            {"statements": {str(stmt_hash): {"evidence": json.loads(stmt_rows[0][0])}}}
        )
    except Exception as err:
        logger.exception(err)
        abort(status=HTTPStatus.INTERNAL_SERVER_ERROR)


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
    curations_list = get_curations(stmt_hash, source_hash=src_hash)
    return jsonify(curations_list)
