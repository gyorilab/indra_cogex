"""
App serving different ways to display the return data from the various
CoGEx apps

# Inspiration:
https://emmaa.indra.bio/evidence?model=covid19&source=model_statement&stmt_hash=26064355888834165&stmt_hash=-10390851375198442&stmt_hash=23381447023984348&stmt_hash=23140517349998103&stmt_hash=5866988047061824&stmt_hash=-35772916439665148&stmt_hash=3092749483304152&stmt_hash=-12269054015102995&stmt_hash=-26969793686025231&stmt_hash=4505325411022432&stmt_hash=-8847304092825154&stmt_hash=31213825008413749&stmt_hash=30141992178380666&stmt_hash=7575179353322188&stmt_hash=-30540958639363159&stmt_hash=31625126241403423&stmt_hash=26394120188094720&stmt_hash=-18911384093106728&stmt_hash=-12860666273291575&stmt_hash=-28802265300330434&stmt_hash=-2430129610717336&stmt_hash=1293890032279598&stmt_hash=3554404786891022&stmt_hash=-11125623700211639&stmt_hash=3687422032419285&stmt_hash=5305586621075360&stmt_hash=389677147118601&stmt_hash=4523113042432100&stmt_hash=-11643556207375872&stmt_hash=-9244908152322434&stmt_hash=11549424046359188&stmt_hash=29182661416104868&stmt_hash=-11508686685241868&stmt_hash=-27089380057920748&stmt_hash=-4984265834938630&stmt_hash=13171603590448017&stmt_hash=5248067513542319&stmt_hash=2633329737788340&stmt_hash=-17848096805003989&stmt_hash=-34885846099815193&stmt_hash=-16296155165635622&stmt_hash=12168088708840873&stmt_hash=29606940247996506&stmt_hash=20208230469852741&stmt_hash=-21459270342254616&stmt_hash=-21459270342254616&stmt_hash=11711788325523194&stmt_hash=-16093215807632509&stmt_hash=30007766770941473&stmt_hash=-1960362999519656
"""
import json
import logging
from collections import defaultdict
from typing import Dict, Iterable, List, Tuple

from flask import Blueprint, Response, abort, jsonify, render_template, request
from flask_jwt_extended import jwt_optional
from indra.assemblers.html.assembler import _format_evidence_text, _format_stmt_text
from indra.statements import Statement
from indra.util.statement_presentation import _get_available_ev_source_counts
from indra_db.client import get_curations, submit_curation
from indra_db.exceptions import BadHashError
from indralab_auth_tools.auth import resolve_auth

from indra_cogex.apps.proxies import client
from indra_cogex.apps.query_web_app import process_result
from indra_cogex.client.curation import get_go_curation_hashes
from indra_cogex.client.queries import get_stmts_for_stmt_hashes

logger = logging.getLogger(__name__)

data_display_blueprint = Blueprint("data_display", __name__)

# Derived types
StmtRow = Tuple[List[Dict], str, str, Dict[str, int], int, List[Dict]]


# Helper functions (move them to a separate file?)
def count_curations(
    curations, stmts_by_hash
) -> Dict[int, Dict[str, defaultdict[str, int]]]:
    correct_tags = ["correct", "act_vs_amt", "hypothesis"]
    cur_counts = {}
    for cur in curations:
        stmt_hash = cur["pa_hash"]
        if stmt_hash not in stmts_by_hash:
            print(f"{stmt_hash} not among {list(stmts_by_hash.keys())}")
            continue
        if stmt_hash not in cur_counts:
            cur_counts[stmt_hash] = {
                "this": defaultdict(int),
                "other": defaultdict(int),
            }
        if cur["tag"] in correct_tags:
            cur_tag = "correct"
        else:
            cur_tag = "incorrect"
        if cur["source_hash"] in [
            evid.get_source_hash() for evid in stmts_by_hash[stmt_hash].evidence
        ]:
            cur_source = "this"
        else:
            cur_source = "other"
        cur_counts[stmt_hash][cur_source][cur_tag] += 1
    return cur_counts


def format_stmts(stmts: Iterable[Statement]) -> List[StmtRow]:
    """Format the statements for display

    Wanted objects:
    - evidence: array of evidence json objects to be passed to <evidence>
    - english: html formatted text
    - hash: as a string
    - sources: source counts
    - total_evidence: total evidence count
    - badges (what is this?) see emmaa_service/api.py::_make_badges() -
        Evidence count:
            {'label': 'evidence', 'num': evid_count, 'color': 'grey',
             'symbol': None, 'title': 'Evidence count for this statement',
             'loc': 'right'},
        Belief:
            {'label': 'belief', 'num': belief, 'color': '#ffc266',
             'symbol': None, 'title': 'Belief score for this statement',
             'loc': 'right'},
        Correct curation count:
            {'label': 'correct_this', 'num': cur_counts['this']['correct'],
             'color': '#28a745', 'symbol':  '\u270E',
             'title': 'Curated as correct in this model'},

    """

    def stmt_to_row(
        st: Statement,
    ) -> List[str]:
        ev_array = json.loads(
            json.dumps(
                _format_evidence_text(
                    st,
                    curation_dict=cur_dict,
                    correct_tags=["correct", "act_vs_amt", "hypothesis"],
                )
            )
        )
        english = _format_stmt_text(st)
        hash_int = st.get_hash()
        sources = _get_available_ev_source_counts(st.evidence)
        total_evidence = len(st.evidence)
        badges = [
            {
                "label": "evidence",
                "num": total_evidence,
                "color": "grey",
                "symbol": None,
                "title": "Evidence count for this statement",
                "loc": "right",
            },
            {
                "label": "belief",
                "num": st.belief,
                "color": "#ffc266",
                "symbol": None,
                "title": "Belief score for this statement",
                "loc": "right",
            },
        ]
        if cur_counts and hash_int in cur_counts:
            num = cur_counts[hash_int]["this"]["correct"]
            badges.append(
                {
                    "label": "correct_this",
                    "num": num,
                    "color": "#28a745",
                    "symbol": "\u270E",
                    "title": f"{num} evidences curated as correct",
                }
            )

        return [
            json.dumps(e)
            for e in [ev_array, english, str(hash_int), sources, total_evidence, badges]
        ]

    all_pa_hashes = [st.get_hash() for st in stmts]
    curations = get_curations(pa_hash=all_pa_hashes)
    cur_dict = defaultdict(list)
    for cur in curations:
        cur_dict[(cur["pa_hash"], cur["source_hash"])].append(
            {"error_type": cur["tag"]}
        )

    stmts_by_hash = {st.get_hash(): st for st in stmts}
    cur_counts = count_curations(curations, stmts_by_hash)
    if cur_counts:
        assert isinstance(
            list(cur_counts.keys())[0], int
        ), f"{list(cur_counts.keys())[0]} is not an int"
        key = list(cur_counts.keys())[0]
        assert isinstance(
            list(cur_counts[key].keys())[0], str
        ), f"{list(cur_counts[key].keys())[0]} is not an str"

    stmt_rows = []
    for stmt in stmts:
        stmt_rows.append(list(stmt_to_row(stmt)))
    return stmt_rows


# Endpoint for testing
@data_display_blueprint.route("/get_stmts", methods=["GET"])
def get_stmts():
    # Get the statements hash from the query string
    try:
        stmt_hash_list = request.args.getlist("stmt_hash", type=int)
        stmts = get_stmts_for_stmt_hashes(stmt_hash_list)
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
        print(stmt_hash_list)
        if not stmt_hash_list:
            abort(Response("Parameter 'stmt_hash' unfilled", status=415))
        stmts = format_stmts(get_stmts_for_stmt_hashes(stmt_hash_list))
        return render_template(
            "data_display/data_display_base.html", stmts=stmts, user_email=email
        )
    except (TypeError, ValueError) as err:
        logger.exception(err)
        abort(Response("Parameter 'stmt_hash' unfilled", status=415))


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


# Curation endpoints
@data_display_blueprint.route("/curate/go/<term>", methods=["GET"])
@jwt_optional
def curate_go(term: str):
    user, roles = resolve_auth(dict(request.args))
    email = user.email if user else ""
    max_results = request.args.get("max_results", type=int, default=25)

    # Get the statements for go term
    try:
        # Example: 'GO:0003677'
        hashes = get_go_curation_hashes(go_term=("GO", term), client=client)
        stmts = get_stmts_for_stmt_hashes(hashes[:max_results])
        form_stmts = format_stmts(stmts)
        return render_template(
            "data_display/data_display_base.html", stmts=form_stmts, user_email=email
        )
    except (TypeError, ValueError) as err:
        logger.exception(err)
        abort(Response("Parameter 'term' unfilled", status=415))


@data_display_blueprint.route("/curate/<hash_val>", methods=["POST"])
@jwt_optional
def submit_curation_endpoint(hash_val: str):
    email = _get_user()
    if not isinstance(email, str):
        return email

    logger.info("Adding curation for statement %s." % hash_val)
    ev_hash = request.json.get("ev_hash")
    source_api = request.json.pop("source", "EMMAA")
    tag = request.json.get("tag")
    ip = request.remote_addr
    text = request.json.get("text")
    is_test = "test" in request.args
    if not is_test:
        assert tag != "test"
        try:
            dbid = submit_curation(
                int(hash_val), tag, email, ip, text, ev_hash, source_api
            )
        except BadHashError as e:
            abort(Response("Invalid hash: %s." % e.bad_hash, 400))
        res = {"result": "success", "ref": {"id": dbid}}
    else:
        res = {"result": "test passed", "ref": None}
    logger.info("Got result: %s" % str(res))
    return jsonify(res)


@data_display_blueprint.route("/curation/list/<stmt_hash>/<src_hash>", methods=["GET"])
def list_curations(stmt_hash, src_hash):
    curations = get_curations(pa_hash=stmt_hash, source_hash=src_hash)
    return jsonify(curations)
