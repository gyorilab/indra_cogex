# -*- coding: utf-8 -*-

"""Processor for Bgee."""

import os
import pickle
from pathlib import Path
from typing import Union

import pyobo

from indra_cogex.representation import Node, Relation
from indra_cogex.sources.processor import Processor


class BgeeProcessor(Processor):
    """Processor for Bgee."""

    name = "bgee"

    def __init__(self, path: Union[None, str, Path] = None):
        """Initialize the Bgee processor.

        :param path: The path to the Bgee dump pickle. If none given, will look in the default location.
        """
        if path is None:
            path = os.path.join(os.path.dirname(__file__), "expressions.pkl")
        elif isinstance(path, str):
            path = Path(path)
        self.rel_type = "expressed_in"
        with open(path, "rb") as fh:
            self.expressions = pickle.load(fh)

    def get_nodes(self):  # noqa:D102
        for context in self.expressions:
            context_ns, context_id = get_context(context)
            yield Node(
                context_ns,
                context_id,
                ["BioEntity"],
                data={"name": pyobo.get_name_by_curie(context_id)},
            )
        for hgnc_id in set.union(*[set(v) for v in self.expressions.values()]):
            yield Node(
                "HGNC",
                hgnc_id,
                ["BioEntity"],
                data={"name": pyobo.get_name("hgnc", hgnc_id)},
            )

    def get_relations(self):  # noqa:D102
        data = {"source": self.name}
        for context, hgnc_ids in self.expressions.items():
            context_ns, context_id = get_context(context)
            for hgnc_id in hgnc_ids:
                yield Relation(
                    "HGNC", hgnc_id, context_ns, context_id, [self.rel_type], data
                )


def get_context(context):
    context_ns, context_id = context.split(":", maxsplit=1)
    if context_ns == "UBERON":
        context_id = f"UBERON:{context_id}"
    return context_ns, context_id
