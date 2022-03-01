# -*- coding: utf-8 -*-

"""An app wrapping the query module of indra_cogex."""
import logging
from inspect import isfunction, signature, Signature
from typing import Callable, Tuple, Type, Iterable, Any, Dict, List, Counter, Mapping

import flask
from docstring_parser import parse
from flask import request, jsonify, abort, Response
from flask_restx import Api, Resource, fields
from more_click import make_web_command

from indra.statements import Evidence, Statement, Agent
from indra_cogex.client.neo4j_client import Neo4jClient
from indra_cogex.client import queries
from indra_cogex.representation import Node

app = flask.Flask(__name__)
api = Api(
    app,
    title="INDRA CoGEx Query API",
    description="REST API for INDRA CoGEx queries",
)

query_ns = api.namespace("CoGEx Queries", "Queries for INDRA CoGEx", path="/api/")
client = Neo4jClient()


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
    "stmt_hash": 12198579805553967,
    "stmt_hashes": [12198579805553967, 30651649296901235],
    "cell_line": ["CCLE", "BT20_BREAST"],
    "target": ["HGNC", "6840"],
    "include_indirect": True,
    "evidence_map": {},
}


def process_result(result) -> Any:
    # Any fundamental type
    if isinstance(result, (int, str, bool)):
        return result
    # Any iterable query
    elif isinstance(result, (Iterable, list, set)):
        list_res = list(result)
        if hasattr(list_res[0], "to_json"):
            list_res = [res.to_json() for res in list_res]
        return list_res
    # Any dict query
    elif isinstance(result, (dict, Mapping, Counter)):
        res_dict = dict(result)
        return {k: process_result(v) for k, v in res_dict.items()}
    else:
        raise TypeError(f"Don't know how to process result of type {type(result)}")


def get_web_return_annotation(sig: Signature) -> Type:
    """Get and translate the return annotation of a function."""
    # Get the return annotation
    return_annotation = sig.return_annotation
    if return_annotation is sig.empty:
        raise ValueError("Forgot to type annotate function")

    # Translate the return annotation:
    # Iterable[Node] -> List[Dict[str, Any]]
    # bool -> Dict[str: bool]
    # Dict[str, List[Evidence]] -> Dict[int, List[Dict[str, Any]]]
    # Iterable[Evidence] -> List[Dict[str, Any]]
    # Iterable[Statement] -> List[Dict[int, Any]]
    # Counter -> Dict[str, int]
    # Iterable[Agent] -> List[Dict[str, Any]]

    if return_annotation is Iterable[Node]:
        return List[Dict[str, Any]]
    elif return_annotation is bool:
        return Dict[str, bool]
    elif return_annotation is Dict[int, List[Evidence]]:
        return Dict[str, List[Dict[str, Any]]]
    elif return_annotation is Iterable[Evidence]:
        return List[Dict[str, Any]]
    elif return_annotation is Iterable[Statement]:
        return List[Dict[str, Any]]
    elif return_annotation is Counter:
        return Dict[str, int]
    elif return_annotation is Iterable[Agent]:
        return List[Dict[str, Any]]
    else:
        return return_annotation


def get_docstring(fun: Callable) -> Tuple[str, str]:
    parsed_doc = parse(fun.__doc__)
    sig = signature(fun)

    full_docstr = """{title}

Parameters
----------
{params}

Returns
-------
{return_str}
"""
    # Get title
    short = parsed_doc.short_description

    param_templ = "{name} : {typing}\n    {description}"

    ret_templ = "{typing}\n    {description}"

    # Get the parameters
    param_list = []
    for param in parsed_doc.params:
        # Skip client, evidence_map,
        if param.arg_name in ("client", "evidence_map"):
            continue

        str_type = str(sig.parameters[param.arg_name].annotation).replace("typing.", "")
        param_list.append(
            param_templ.format(
                name=param.arg_name, typing=str_type, description=param.description
            )
        )
    params = "\n\n".join(param_list)

    return_str = ret_templ.format(
        typing=str(get_web_return_annotation(sig)).replace("typing.", ""),
        description=parsed_doc.returns.description,
    )

    return short, full_docstr.format(
        title=short,
        params=params,
        return_str=return_str,
    )


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

    model_name = f"{func_name} model"

    # Create query model, separate between one and two parameter expectations
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
                result = func_mapping[self.func_name](**json_dict, client=client)
                # Any 'is' type query
                if isinstance(result, bool):
                    return jsonify({self.func_name: result})
                else:
                    return jsonify(process_result(result))
            except TypeError as err:
                logger.error(err)
                abort(Response(str(err), 415))

        post.__doc__ = fixed_doc


cli = make_web_command(app=app)


if __name__ == "__main__":
    cli()
