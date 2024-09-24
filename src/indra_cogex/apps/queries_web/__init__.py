# -*- coding: utf-8 -*-

"""An app wrapping the query module of indra_cogex.

The endpoints are created dynamically based on the functions in the following modules:
- indra_cogex.client.queries
- indra_cogex.client.subnetwork
- indra_cogex.analysis.metabolite_analysis
- indra_cogex.analysis.gene_analysis
"""

import logging
from http import HTTPStatus
from inspect import isfunction, signature

from flask import jsonify, request
from flask_restx import Api, Resource, abort, fields

from indra_cogex.apps.proxies import client
from indra_cogex.client import queries, subnetwork
from indra_cogex.analysis import metabolite_analysis, gene_analysis

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

from flask_restx import fields

examples_dict = {
    "tissue": fields.List(fields.String, example=["UBERON", "UBERON:0001162"]),
    "gene": fields.List(fields.String, example=["HGNC", "9896"]),
    "go_term": fields.List(fields.String, example=["GO", "GO:0000978"]),
    "drug": fields.List(fields.String, example=["CHEBI", "CHEBI:27690"]),
    "drugs": fields.Raw(
        example={"CHEBI:27690": "Drug 1", "CHEBI:114785": "Drug 2"}
    ),
    "disease": fields.List(fields.String, example=["MESH", "D007855"]),
    "trial": fields.List(fields.String, example=["CLINICALTRIALS", "NCT00000114"]),
    "genes": fields.Raw(
        example={"hgnc:1000": "BCL5", "hgnc:100": "ASIC1"}
    ),
    "pathway": fields.List(fields.String, example=["WIKIPATHWAYS", "WP5037"]),
    "side_effect": fields.List(fields.String, example=["UMLS", "C3267206"]),
    "term": fields.List(fields.String, example=["MESH", "D007855"]),
    "parent": fields.List(fields.String, example=["MESH", "D007855"]),
    "mesh_term": fields.List(fields.String, example=["MESH", "D015002"]),
    "pmid_term": fields.List(fields.String, example=["PUBMED", "34634383"]),
    "paper_term": fields.List(fields.String, example=["PUBMED", "34634383"]),
    "pmids": fields.List(fields.String, example=["20861832", "19503834"]),
    "include_child_terms": fields.Boolean(example=True),
    "stmt_hash": fields.String(example="12198579805553967"),
    "stmt_hashes": fields.List(fields.String, example=["12198579805553967", "30651649296901235"]),
    "cell_line": fields.List(fields.String, example=["CCLE", "BT20_BREAST"]),
    "target": fields.List(fields.String, example=["HGNC", "6840"]),
    "targets": fields.Raw(
        example={"HGNC:6840": "Target 1", "HGNC:1097": "Target 2"}
    ),
    "include_indirect": fields.Boolean(example=True),
    "filter_medscan": fields.Boolean(example=True),
    "limit": fields.Integer(example=30),
    "evidence_limit": fields.Integer(example=30),
    "nodes": fields.List(
        fields.List(fields.String),
        example=[["FPLX", "MEK"], ["FPLX", "ERK"]]
    ),
    "offset": fields.Integer(example=1),

    # Analysis API
    "metabolites": fields.Raw(
        example={"CHEBI:12345": "Metabolite 1", "CHEBI:67890": "Metabolite 2"}
    ),
    "method": fields.String(example="bonferroni"),
    "alpha": fields.Float(example=0.05, min=0, max=1),
    "keep_insignificant": fields.Boolean(example=False),
    "minimum_evidence_count": fields.Integer(example=2),
    "minimum_belief": fields.Float(example=0.7, min=0, max=1),
    "ec_code": fields.String(example="3.2.1.4"),
    "chebi_ids": fields.Raw(
        example={"CHEBI:27690": "Chemical 1", "CHEBI:114785": "Chemical 2"}
    ),
    "positive_genes": fields.Raw(
        example={
            "HGNC:10354": "Gene A",
            "HGNC:4141": "Gene B",
            "HGNC:1692": "Gene C"
        }
    ),
    "negative_genes": fields.Raw(
        example={
            "HGNC:5471": "Gene X",
            "HGNC:11763": "Gene Y",
            "HGNC:2192": "Gene Z"
        }
    ),
    "gene_names": fields.List(fields.String, example=["BRCA1", "TP53", "EGFR"]),
    "log_fold_change": fields.List(fields.Float, example=[1.5, -0.8, 2.1]),
    "species": fields.String(example="human"),
    "permutations": fields.Integer(example=100),
    "source": fields.String(example="go"),
}

# Parameters to always skip in the examples and in the documentation
SKIP_GLOBAL = {"client", "return_evidence_counts", "kwargs",
               "subject_prefix", "object_prefix", "file_path"}

# Parameters to skip for specific functions
SKIP_ARGUMENTS = {
    "get_stmts_for_stmt_hashes": {"return_evidence_counts", "evidence_map"},
    "get_evidences_for_stmt_hash": {"remove_medscan"},
    "get_evidences_for_stmt_hashes": {"remove_medscan"},
}

# This is the list of functions to be included
# To add a new function, make sure it is part of __all__ in the respective module or is
# listed explicitly below and properly documented in its docstring as well as having
# example values for its parameters in the examples_dict above.
module_functions = (
        [(queries, fn) for fn in queries.__all__] +
        [(subnetwork, fn) for fn in ["indra_subnetwork_relations", "indra_subnetwork_meta"]] +
        [(metabolite_analysis, fn) for fn in ["metabolite_discrete_analysis"]] +
        [(gene_analysis, fn) for fn in ["discrete_analysis", "signed_analysis", "continuous_analysis"]]
)

# Maps function names to the actual functions
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

    short_doc, fixed_doc = get_docstring(
        func, skip_params=SKIP_GLOBAL | SKIP_ARGUMENTS.get(func_name, set())
    )

    param_names = list(func_sig.parameters.keys())
    param_names.remove("client")

    model_name = f"{func_name}_model"

    for param_name in param_names:
        if param_name in SKIP_GLOBAL:
            continue
        if param_name in SKIP_ARGUMENTS.get(func_name, set()):
            continue
        if param_name not in examples_dict:
            raise KeyError(
                f"Missing example for parameter '{param_name}' in function '{func_name}'"
            )

    # Get the parameters name for the other parameter that is not 'client'
    query_model = api.model(
        model_name,
        {
            param_name: examples_dict[param_name]
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
