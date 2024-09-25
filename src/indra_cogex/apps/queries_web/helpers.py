from inspect import Signature, signature
from typing import (
    Any,
    Callable,
    Counter,
    Dict,
    Iterable,
    List,
    Mapping,
    Tuple,
    Type,
    Optional,
    Set,
)

import pandas as pd
from docstring_parser import parse
from indra.statements import Agent, Evidence, Statement

from indra_cogex.representation import Node

__all__ = [
    "parse_json",
    "process_result",
    "get_web_return_annotation",
    "get_docstring",
    "ParseError",
]


class ParseError(ValueError):
    """Raised when the JSON cannot be parsed or is not valid."""


MAX_NODES = 400


def parse_json(query_json: Dict[str, Any]) -> Dict[str, Any]:
    """Parse the incoming query

    Parameters
    ----------
    query_json :
        The incoming query as a dictionary

    Returns
    -------
    :
        The parsed query
    """
    parsed_query = {}
    for key, value in query_json.items():
        if key in ("stmt_hashes", "stmt_hash"):
            if isinstance(value, str):
                parsed_query[key] = int(value)
            elif isinstance(value, list):
                parsed_query[key] = [int(v) for v in value]
            else:
                raise ParseError(f"{key} must be a string or list of strings")
        elif key == "nodes":
            if isinstance(value, list):
                if len(value) > MAX_NODES:
                    raise ValueError(f"Number of {key} must be less than {MAX_NODES}")
                parsed_query[key] = value
            else:
                raise ParseError(f"{key} must be a list")
        else:
            parsed_query[key] = value

    return parsed_query


def process_result(result) -> Any:
    """Make the result of a query JSON-serializable.

    Parameters
    ----------
    result :
        The result of a query

    Returns
    -------
    :
        The processed result
    """
    # Any fundamental type
    if isinstance(result, (int, str, bool, float)):
        return result
    # Any dict query
    elif isinstance(result, (dict, Mapping, Counter)):
        res_dict = dict(result)
        return {k: process_result(v) for k, v in res_dict.items()}
    # DataFrames
    elif isinstance(result, pd.DataFrame):
        return result.to_dict(orient="records")
    # Any iterable query
    elif isinstance(result, (Iterable, list, set)):
        list_res = list(result)
        # Check for empty list
        if list_res and hasattr(list_res[0], "to_json"):
            list_res = [res.to_json() for res in list_res]
        return list_res
    else:
        raise TypeError(f"Don't know how to process result of type {type(result)}")


def get_web_return_annotation(sig: Signature) -> Type:
    """Get and translate the return annotation of a function

    Parameters
    ----------
    sig :
        The signature of the function

    Returns
    -------
    :
        The return annotation of the function
    """
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
    # Agent -> Dict[str, Any]
    # Mapping[str, Iterable[indra.statements.agent.Agent]]
    #   -> Dict[str, List[Dict[str, Any]]]

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
    elif return_annotation is Agent:
        return Dict[str, Any]
    elif return_annotation is Mapping[str, Iterable[Agent]]:
        return Dict[str, List[Dict[str, Any]]]
    else:
        return return_annotation


def get_docstring(
    fun: Callable, skip_params: Optional[Set[str]] = None
) -> Tuple[str, str]:
    """Get the docstring of a function

    Parameters
    ----------
    fun :
        The function whose docstring is to be retrieved
    skip_params :
        The parameters to skip docstring generation for

    Returns
    -------
    :
        The docstring of the function
    """
    parsed_doc = parse(fun.__doc__)
    if parsed_doc.returns is None:
        raise ValueError(
            f"Forgot to document return value in docstring of {fun.__name__}"
        )
    if parsed_doc.params is None:
        raise ValueError(
            f"Forgot to document parameters in docstring of {fun.__name__}"
        )
    sig = signature(fun)
    if sig.return_annotation is sig.empty:
        raise ValueError("Forgot to type annotate function")

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
        if param.arg_name in skip_params:
            continue

        if param.arg_name == "stmt_hash":
            annot = str
        elif param.arg_name == "stmt_hashes":
            annot = List[str]
        else:
            annot = sig.parameters[param.arg_name].annotation
        str_type = str(annot).replace("typing.", "")

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
