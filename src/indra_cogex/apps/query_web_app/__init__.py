# -*- coding: utf-8 -*-

"""An app wrapping the query module of indra_cogex."""
import logging

import flask
from flask import request, abort, Response, jsonify
from more_click import make_web_command

from indra_cogex.client.neo4j_client import Neo4jClient
from indra_cogex.client.queries import *

app = flask.Flask(__name__)

client = Neo4jClient()


logger = logging.getLogger(__name__)


@app.route('/get_genes_in_tissue', methods=['POST'])
def genes_in_tissue():
    """Get genes for a disease."""
    if request.json is None:
        abort(Response('Missing application/json header.', 415))

    disease_name = request.json.get('disease_name')
    if disease_name is None:
        abort(Response("Parameter 'disease_name' not provided", 415))
    logger.info('Getting genes for disease %s' % disease_name)
    genes = get_genes_in_tissue(client, disease_name)
    logger.info('Found %d genes' % len(genes))
    return jsonify([g.to_json() for g in genes])


cli = make_web_command(app=app)


if __name__ == '__main__':
    cli()
