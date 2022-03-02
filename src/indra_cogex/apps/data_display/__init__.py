"""
App serving different ways to display the return data from the various
CoGEx apps

# Inspiration:
https://emmaa.indra.bio/evidence?model=covid19&source=model_statement&stmt_hash=26064355888834165&stmt_hash=-10390851375198442&stmt_hash=23381447023984348&stmt_hash=23140517349998103&stmt_hash=5866988047061824&stmt_hash=-35772916439665148&stmt_hash=3092749483304152&stmt_hash=-12269054015102995&stmt_hash=-26969793686025231&stmt_hash=4505325411022432&stmt_hash=-8847304092825154&stmt_hash=31213825008413749&stmt_hash=30141992178380666&stmt_hash=7575179353322188&stmt_hash=-30540958639363159&stmt_hash=31625126241403423&stmt_hash=26394120188094720&stmt_hash=-18911384093106728&stmt_hash=-12860666273291575&stmt_hash=-28802265300330434&stmt_hash=-2430129610717336&stmt_hash=1293890032279598&stmt_hash=3554404786891022&stmt_hash=-11125623700211639&stmt_hash=3687422032419285&stmt_hash=5305586621075360&stmt_hash=389677147118601&stmt_hash=4523113042432100&stmt_hash=-11643556207375872&stmt_hash=-9244908152322434&stmt_hash=11549424046359188&stmt_hash=29182661416104868&stmt_hash=-11508686685241868&stmt_hash=-27089380057920748&stmt_hash=-4984265834938630&stmt_hash=13171603590448017&stmt_hash=5248067513542319&stmt_hash=2633329737788340&stmt_hash=-17848096805003989&stmt_hash=-34885846099815193&stmt_hash=-16296155165635622&stmt_hash=12168088708840873&stmt_hash=29606940247996506&stmt_hash=20208230469852741&stmt_hash=-21459270342254616&stmt_hash=-21459270342254616&stmt_hash=11711788325523194&stmt_hash=-16093215807632509&stmt_hash=30007766770941473&stmt_hash=-1960362999519656
"""

from flask import request, jsonify, abort, Response, Flask
from more_click import make_web_command
from indra_cogex.apps.query_web_app import process_result
from indra_cogex.client.queries import get_stmts_for_stmt_hashes

# Setup Flask app
app = Flask(__name__)


# Fake endpoint for testing
@app.route('/get_stmts', methods=['GET'])
def get_stmts():
    # Get the statements hash from the query string
    try:
        stmt_hash = int(request.args.get('stmt_hash'))
        stmts = get_stmts_for_stmt_hashes([stmt_hash])
        return jsonify(process_result(stmts))
    except (TypeError, ValueError):
        abort(Response("Parameter 'stmt_hash' unfilled", status=415))


# Create runnable cli command
cli = make_web_command(app)

if __name__ == '__main__':
    cli()
