"""Function metadata extraction for MCP server.

Extracts type information, parameter details, and category descriptions
from query functions.
"""
from functools import lru_cache
from typing import Dict, Any, Optional, NamedTuple, Tuple
from inspect import signature

__all__ = [
    "FunctionMetadata",
    "build_function_registry",
]


class FunctionMetadata(NamedTuple):
    """Metadata for a single query function.

    Attributes
    ----------
    name : str
        Function name
    description : str
        Short description extracted from docstring
    full_docstring : str
        Complete formatted docstring
    category : str
        Category name (e.g., 'gene_expression', 'drug_targets')
    category_description : str
        Description of the category
    module_name : str
        Name of the module containing the function
    parameters : Dict[str, Any]
        Parameter details: {param_name: {type, required, default, example}}
    return_type : str
        String representation of the return type
    """
    name: str
    description: str
    full_docstring: str
    category: str
    category_description: str
    module_name: str
    parameters: Dict[str, Any]
    return_type: str


def build_function_registry(
    module_functions,
    func_mapping: Dict,
    function_categories: Dict,
    category_descriptions: Dict,
    examples_dict: Dict,
    skip_global: set,
    skip_arguments: Dict,
    get_docstring_func,
) -> Dict[str, FunctionMetadata]:
    """Build function registry with metadata.

    Parameters
    ----------
    module_functions : list
        List of (module, function_name) tuples
    func_mapping : dict
        Mapping of function names to function objects
    function_categories : dict
        Category definitions with namespace and function lists
    category_descriptions : dict
        Descriptions for each category
    examples_dict : dict
        Parameter examples for documentation
    skip_global : set
        Global parameters to skip (e.g., 'client', 'kwargs')
    skip_arguments : dict
        Function-specific parameters to skip
    get_docstring_func : callable
        Function to extract docstrings (from helpers module)

    Returns
    -------
    :
        Registry mapping function names to metadata
    """
    registry = {}

    for module, func_name in module_functions:
        if func_name not in func_mapping:
            continue

        func = func_mapping[func_name]
        func_sig = signature(func)

        # Find category
        category = None
        category_desc = ""
        for cat_name, cat_info in function_categories.items():
            if func_name in cat_info['functions']:
                category = cat_name
                category_desc = category_descriptions.get(cat_name, "")
                break

        if category is None:
            category = 'uncategorized'

        # Get docstring
        try:
            short_desc, full_doc = get_docstring_func(
                func,
                skip_params=skip_global | skip_arguments.get(func_name, set())
            )
        except (ValueError, AttributeError):
            # Skip functions without proper documentation
            continue

        # Build parameter details
        param_details = {}
        for param_name, param_obj in func_sig.parameters.items():
            # Skip client and other global parameters
            if param_name in skip_global:
                continue
            if param_name in skip_arguments.get(func_name, set()):
                continue

            # Get example from examples_dict
            example = None
            if param_name in examples_dict:
                ex = examples_dict[param_name]
                # Handle function-specific examples
                if isinstance(ex, dict):
                    example = ex.get(func_name, ex.get("default"))
                else:
                    example = ex

            param_details[param_name] = {
                "type": str(param_obj.annotation).replace("typing.", ""),
                "required": param_obj.default == param_obj.empty,
                "default": None if param_obj.default == param_obj.empty else param_obj.default,
                "example": example
            }

        # Create metadata entry
        registry[func_name] = FunctionMetadata(
            name=func_name,
            description=short_desc,
            full_docstring=full_doc,
            category=category,
            category_description=category_desc,
            module_name=module.__name__,
            parameters=param_details,
            return_type=str(func_sig.return_annotation).replace("typing.", "")
        )

    return registry
