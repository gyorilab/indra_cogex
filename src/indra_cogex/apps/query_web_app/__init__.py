# -*- coding: utf-8 -*-

"""An app wrapping the query module of indra_cogex."""

import logging
from inspect import isfunction, signature

from flask import Response, abort, jsonify, request
from flask_restx import Api, Resource, fields

from indra_cogex.apps.proxies import client
from indra_cogex.client import queries

from .helpers import get_docstring, parse_json, process_result

api = Api(
    title="INDRA CoGEx Query API",
    description="REST API for INDRA CoGEx queries",
    doc="/apidocs",
)

query_ns = api.namespace("CoGEx Queries", "Queries for INDRA CoGEx", path="/api/")

logger = logging.getLogger(__name__)

examples_dict = {
    "tissue": ["UBERON", "UBERON:0002349"],
    "gene": ["HGNC", "9896"],
    "go_term": ["GO", "GO:0000978"],
    "drug": ["CHEBI", "CHEBI:27690"],
    "disease": ["MESH", "D007855"],
    "trial": ["CLINICALTRIALS", "NCT00000114"],
    "genes": [["HGNC", "1097"], ["HGNC", "6407"]],
    "pathway": ["WIKIPATHWAYS", "WP5037"],
    "side_effect": ["UMLS", "C3267206"],
    "term": ["MESH", "D007855"],
    "parent": ["MESH", "D007855"],
    "mesh_term": ["MESH", "D015002"],
    "pmid_term": ["PUBMED", "27890007"],
    "include_child_terms": True,
    # NOTE: statement hashes are too large to be int for JavaScript
    "stmt_hash": "12198579805553967",
    "stmt_hashes": ["12198579805553967", "30651649296901235"],
    "cell_line": ["CCLE", "BT20_BREAST"],
    "target": ["HGNC", "6840"],
    "include_indirect": True,
    "evidence_map": {},
    "filter_medscan": True,
}

func_mapping = {fname: getattr(queries, fname) for fname in queries.__all__}

# Create resource for each query function
for func_name in queries.__all__:
    if not isfunction(getattr(queries, func_name)) or func_name == "get_schema_graph":
        continue

    func = getattr(queries, func_name)
    func_sig = signature(func)
    client_param = func_sig.parameters.get("client")
    if client_param is None:
        continue

    short_doc, fixed_doc = get_docstring(func)

    param_names = list(func_sig.parameters.keys())
    param_names.remove("client")

    model_name = f"{func_name}_model"

    # Create query model, separate between one and two parameter expectations
    try:
        if len(func_sig.parameters) == 2:
            # Get the parameters name for the other parameter that is not 'client'
            query_model = api.model(
                model_name,
                {
                    param_names[0]: fields.List(
                        fields.String, example=examples_dict[param_names[0]]
                    )
                },
            )
        elif len(func_sig.parameters) == 3:

            query_model = api.model(
                model_name,
                {
                    param_names[0]: fields.List(
                        fields.String, example=examples_dict[param_names[0]]
                    ),
                    param_names[1]: fields.List(
                        fields.String, example=examples_dict[param_names[1]]
                    ),
                },
            )
        else:
            raise ValueError(
                f"Query function {func_name} has an unexpected number of "
                f"parameters ({len(func_sig.parameters)})"
            )
    except KeyError as err:
        raise KeyError(f"No examples for {func_name}, please add one") from err

    @query_ns.expect(query_model)
    @query_ns.route(f"/{func_name}", doc={"summary": short_doc})
    class QueryResource(Resource):
        """A resource for a query."""

        func_name = func_name

        def post(self):
            """Get a query."""
            json_dict = request.json
            if json_dict is None:
                abort(Response("Missing application/json header.", 415))
            try:
                parsed_query = parse_json(json_dict)
                result = func_mapping[self.func_name](**parsed_query, client=client)
                # Any 'is' type query
                if isinstance(result, bool):
                    return jsonify({self.func_name: result})
                else:
                    return jsonify(process_result(result))
            except TypeError as err:
                logger.error(err)
                abort(Response(str(err), 415))

            except Exception as err:
                logger.error(err)
                abort(Response(str(err), 500))

        post.__doc__ = fixed_doc
