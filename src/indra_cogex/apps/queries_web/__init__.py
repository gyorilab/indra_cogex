# -*- coding: utf-8 -*-

"""An app wrapping the query module of indra_cogex."""

import logging
from http import HTTPStatus
from inspect import isfunction, signature

from flask import jsonify, request
from flask_restx import Api, Resource, abort, fields

from indra_cogex.apps.proxies import client
from indra_cogex.client import queries, subnetwork

from .helpers import ParseError, get_docstring, parse_json, process_result

__all__ = [
    "api",
    "query_ns",
]

logger = logging.getLogger(__name__)

api = Api(
    title="INDRA CoGEx Query API",
    description="REST API for INDRA CoGEx queries",
    doc="/apidocs",
)

query_ns = api.namespace("CoGEx Queries", "Queries for INDRA CoGEx", path="/api/")

examples_dict = {
    "tissue": ["UBERON", "UBERON:0001162"],
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
    "limit": 30,
    "evidence_limit": 30,
    "nodes": [["FPLX", "MEK"], ["FPLX", "ERK"]],
}

SKIP_GLOBAL = {"return_evidence_counts", "kwargs", "subject_prefix", "object_prefix"}
SKIP_ARGUMENTS = {"get_stmts_for_stmt_hashes": {"return_evidence_counts"}}

# This is the list of functions to be included
module_functions = [(queries, fn) for fn in queries.__all__] + [
    (subnetwork, fn) for fn in ["indra_subnetwork_relations"]
]

func_mapping = {fname: getattr(module, fname) for module, fname in module_functions}

# Create resource for each query function
for module, func_name in module_functions:
    if not isfunction(getattr(module, func_name)) or func_name == "get_schema_graph":
        continue

    func = getattr(module, func_name)
    func_sig = signature(func)
    client_param = func_sig.parameters.get("client")
    if client_param is None:
        continue

    short_doc, fixed_doc = get_docstring(func)

    param_names = list(func_sig.parameters.keys())
    param_names.remove("client")

    model_name = f"{func_name}_model"

    for param_name in param_names:
        if param_name in SKIP_GLOBAL:
            continue
        if param_name in SKIP_ARGUMENTS.get(func_name, []):
            continue
        if param_name not in examples_dict:
            raise KeyError(
                f"Missing example for parameter {param_name} in function {func_name}"
            )

    # Get the parameters name for the other parameter that is not 'client'
    query_model = api.model(
        model_name,
        {
            param_name: fields.List(fields.String, example=examples_dict[param_name])
            for param_name in param_names
            if param_name not in SKIP_GLOBAL
            and param_name not in SKIP_ARGUMENTS.get(func_name, [])
        },
    )

    @query_ns.expect(query_model)
    @query_ns.route(f"/{func_name}", doc={"summary": short_doc})
    class QueryResource(Resource):
        """A resource for a query."""

        func_name = func_name

        def post(self):
            """Get a query."""
            json_dict = request.json
            if json_dict is None:
                abort(
                    code=HTTPStatus.UNSUPPORTED_MEDIA_TYPE,
                    message="Missing application/json header or json body",
                )

            try:
                parsed_query = parse_json(json_dict)
                result = func_mapping[self.func_name](**parsed_query, client=client)
                # Any 'is' type query
                if isinstance(result, bool):
                    return jsonify({self.func_name: result})
                else:
                    return jsonify(process_result(result))

            except ParseError as err:
                logger.error(err)
                abort(code=HTTPStatus.UNSUPPORTED_MEDIA_TYPE, message=str(err))

            except ValueError as err:
                logger.error(err)
                abort(code=HTTPStatus.BAD_REQUEST, message=str(err))

            except Exception as err:
                logger.error(err)
                abort(code=HTTPStatus.INTERNAL_SERVER_ERROR)

        post.__doc__ = fixed_doc
