# -*- coding: utf-8 -*-

"""An app wrapping the query module of indra_cogex."""
import logging
from inspect import isfunction, signature
from typing import Callable, Tuple, Union

import flask
from docstring_parser import parse
from flask import request, Response, jsonify
from flask_restx import Api, Resource, fields, abort
from more_click import make_web_command

from indra_cogex.client.neo4j_client import Neo4jClient
from indra_cogex.client import queries


app = flask.Flask(__name__)
api = Api(
    app, title="INDRA CoGEx Query API", description="REST API for INDRA CoGEx queries"
)

query_ns = api.namespace("CoGEx Queries", "Queries for INDRA CoGEx", path="/api/")
client = Neo4jClient()


logger = logging.getLogger(__name__)

Tup = Tuple[str, str]
TupOfTups = Tuple[Tup, ...]


def get_docstring(fun: Callable) -> Tuple[str, str]:
    parsed_doc = parse(fun.__doc__)

    long = parsed_doc.short_description + "\n\n"

    if parsed_doc.long_description:
        long += parsed_doc.long_description + "\n\n"

    long += "Parameters\n----------\n"
    for param in parsed_doc.params:
        long += param.arg_name + ": " + param.description + "\n"
    long += "\n"
    long += "Returns\n-------\n"
    long += parsed_doc.returns.description

    return parsed_doc.short_description, parsed_doc.long_description


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

    short_doc, doc = get_docstring(func)

    param_names = list(func_sig.parameters.keys())
    param_names.remove("client")

    name_title = func_name.replace("_", " ").title()

    # Create query model, separate between one and two parameter expectations
    if len(func_sig.parameters) == 2:
        # Get the parameters name for the other parameter that is not 'client'
        query_model = api.model(
            f"{func_name}_endpoint", {param_names[0]: fields.List(fields.String)}
        )
    elif len(func_sig.parameters) == 3:

        query_model = api.model(
            f"{func_name}_endpoint",
            {
                param_names[0]: fields.List(fields.String),
                param_names[1]: fields.List(fields.String),
            },
        )
    else:
        raise ValueError(
            f"Query function {func_name} has an unexpected number of "
            f"parameters ({len(func_sig.parameters)})"
        )

    @query_ns.expect(query_model)
    @query_ns.route(f"/{func_name}", doc={"summary": "This is the summary"})
    class QueryResource(Resource):
        """A resource for a query."""

        func_name = func_name

        def post(self):
            """Get a query."""
            json_dict = request.json
            result = func_mapping[self.func_name](**json_dict, client=client)
            return jsonify(result)

        post.__doc__ = "This is the full __doc__"


cli = make_web_command(app=app)


if __name__ == "__main__":
    cli()
