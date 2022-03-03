"""
App serving different ways to display the return data from the various
CoGEx apps

# Inspiration:
https://emmaa.indra.bio/evidence?model=covid19&source=model_statement&stmt_hash=26064355888834165&stmt_hash=-10390851375198442&stmt_hash=23381447023984348&stmt_hash=23140517349998103&stmt_hash=5866988047061824&stmt_hash=-35772916439665148&stmt_hash=3092749483304152&stmt_hash=-12269054015102995&stmt_hash=-26969793686025231&stmt_hash=4505325411022432&stmt_hash=-8847304092825154&stmt_hash=31213825008413749&stmt_hash=30141992178380666&stmt_hash=7575179353322188&stmt_hash=-30540958639363159&stmt_hash=31625126241403423&stmt_hash=26394120188094720&stmt_hash=-18911384093106728&stmt_hash=-12860666273291575&stmt_hash=-28802265300330434&stmt_hash=-2430129610717336&stmt_hash=1293890032279598&stmt_hash=3554404786891022&stmt_hash=-11125623700211639&stmt_hash=3687422032419285&stmt_hash=5305586621075360&stmt_hash=389677147118601&stmt_hash=4523113042432100&stmt_hash=-11643556207375872&stmt_hash=-9244908152322434&stmt_hash=11549424046359188&stmt_hash=29182661416104868&stmt_hash=-11508686685241868&stmt_hash=-27089380057920748&stmt_hash=-4984265834938630&stmt_hash=13171603590448017&stmt_hash=5248067513542319&stmt_hash=2633329737788340&stmt_hash=-17848096805003989&stmt_hash=-34885846099815193&stmt_hash=-16296155165635622&stmt_hash=12168088708840873&stmt_hash=29606940247996506&stmt_hash=20208230469852741&stmt_hash=-21459270342254616&stmt_hash=-21459270342254616&stmt_hash=11711788325523194&stmt_hash=-16093215807632509&stmt_hash=30007766770941473&stmt_hash=-1960362999519656
"""
import json
import logging
from collections import defaultdict
from typing import List, Iterable

from flask import request, jsonify, abort, Response, Flask, render_template
from more_click import make_web_command

from indra_db.client import get_curations
from indra.statements import Statement
from indra.assemblers.html.assembler import _format_stmt_text, _format_evidence_text
from indra.util.statement_presentation import _get_available_ev_source_counts
from indra_cogex.apps.query_web_app import process_result
from indra_cogex.client.queries import get_stmts_for_stmt_hashes


logger = logging.getLogger(__name__)


# Setup Flask app
app = Flask(__name__)


# Helper functions (move them to a separate file?)
def count_curations(curations, stmts_by_hash):
    correct_tags = ["correct", "act_vs_amt", "hypothesis"]
    cur_counts = {}
    for cur in curations:
        stmt_hash = str(cur["pa_hash"])
        if stmt_hash not in stmts_by_hash:
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


def format_stmts(stmts: Iterable[Statement]):
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
    """

    def stmt_to_row(
        st: Statement,
    ) -> List[str]:
        ev_array = json.loads(
            json.dumps(
                _format_evidence_text(
                    st,
                    curation_dict=cur_counts,
                    correct_tags=["correct", "act_vs_amt", "hypothesis"],
                )
            )
        )
        english = _format_stmt_text(st)
        hash_str = str(st.get_hash())
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
        return [
            json.dumps(e)
            for e in [ev_array, english, hash_str, sources, total_evidence, badges]
        ]

    all_pa_hashes = [st.get_hash() for st in stmts]
    stmts_by_hash = {st.get_hash(): st for st in stmts}
    curations = get_curations(pa_hash=all_pa_hashes)
    cur_dict = defaultdict(list)
    for cur in curations:
        cur_dict[(cur["pa_hash"], cur["source_hash"])].append(
            {"error_type": cur["tag"]}
        )
    cur_counts = count_curations(curations, stmts_by_hash)

    stmt_rows = []
    for stmt in stmts:
        stmt_rows.append(list(stmt_to_row(stmt)))
    return stmt_rows


# Endpoint for testing
@app.route("/get_stmts", methods=["GET"])
def get_stmts():
    # Get the statements hash from the query string
    try:
        stmt_hash = int(request.args.get("stmt_hash"))
        stmts = get_stmts_for_stmt_hashes([stmt_hash])
        return jsonify(process_result(stmts))
    except (TypeError, ValueError) as err:
        logger.exception(err)
        abort(Response("Parameter 'stmt_hash' unfilled", status=415))


# Serve the statement display template
@app.route("/statement_display", methods=["GET"])
def statement_display():
    # Get the statements hash from the query string
    try:
        stmt_hash = int(request.args.get("stmt_hash", 0))
        if stmt_hash == 0:
            abort(Response("Parameter 'stmt_hash' unfilled", status=415))
        stmts = get_stmts_for_stmt_hashes([stmt_hash])[:10]
        stmts = format_stmts(stmts)
        return render_template("data_display_base.html", stmts=stmts)
    except (TypeError, ValueError) as err:
        logger.exception(err)
        abort(Response("Parameter 'stmt_hash' unfilled", status=415))


# Curation endpoint
@app.route("/curate/<hash_val>", methods=["POST"])
def submit_curation_endpoint(hash_val: str):
    # For now just return a fake 401 error
    res_dict = {"result": "failure", "reason": "Invalid Credentials"}
    return jsonify(res_dict), 401


# Create runnable cli command
cli = make_web_command(app)

if __name__ == "__main__":
    cli()
