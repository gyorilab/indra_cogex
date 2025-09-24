# -*- coding: utf-8 -*-

"""Processor for Bgee."""
import logging
from collections import defaultdict
import pickle
from pathlib import Path
from typing import Union, Iterable, Tuple

import pandas
import pyobo
import pystow
from tqdm import tqdm
from indra.databases import hgnc_client

from indra_cogex.representation import Node, Relation
from indra_cogex.sources.processor import Processor


logger = logging.getLogger(__name__)


class BgeeProcessor(Processor):
    """Processor for Bgee."""

    name = "bgee"
    node_types = ["BioEntity"]

    def __init__(self, path: Union[None, str, Path] = None):
        """Initialize the Bgee processor.

        :param path: The path to the Bgee dump pickle. If none given, will look in the default location.
        """
        if path is None:
            path = pystow.join("indra", "cogex", "bgee", name="expressions.pkl")
        elif isinstance(path, str):
            path = Path(path)
        self.rel_type = "expressed_in"
        self.expressions = get_expressions(path)

    def get_nodes(self) -> Iterable[Node]:  # noqa:D102
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

    def get_relations(self) -> Iterable[Relation]:  # noqa:D102
        data = {"source": self.name}
        for context, hgnc_ids in self.expressions.items():
            context_ns, context_id = get_context(context)
            for hgnc_id in hgnc_ids:
                yield Relation(
                    "HGNC", hgnc_id, context_ns, context_id, self.rel_type, data
                )


def get_expressions(fname):
    if fname.exists():
        with open(fname, "rb") as fh:
            return pickle.load(fh)
    else:
        url = (
            "https://bgee.org/ftp/bgee_v15_2/download/calls/expr_calls/"
            "Homo_sapiens_expr_simple.tsv.gz"
        )
        logger.info("Getting source tsv file")
        df = pandas.read_csv(url, sep="\t")

        # Filter to "present" Expression only (see
        # https://bgee.org/support/gene-expression-calls#expression-column-9)
        logger.info("Filtering to 'present' expressions")
        df = df[df["Expression"] == "present"]

        # Filter out rows where "Anatomical entity ID" contains '∩', e.g.
        # "UBERON:0000991 ∩ CL:0000670". They should only be ~1 % of all rows.
        logger.info("Filtering out identifiers containing '∩'")
        df = df[~df["Anatomical entity ID"].str.contains("∩")]

        expression = defaultdict(set)
        for _, row in tqdm(
            df.iterrows(), total=df.shape[0], desc="Getting expression mappings"
        ):
            hgnc_id = hgnc_client.get_hgnc_id(row["Gene name"])
            if not hgnc_id:
                continue
            expression[row["Anatomical entity ID"]].add(hgnc_id)
        with open(fname, "wb") as fh:
            pickle.dump(expression, fh)
        return expression


def get_context(context) -> Tuple[str, str]:
    context_ns, context_id = context.split(":", maxsplit=1)
    if context_ns == "UBERON":
        context_id = f"UBERON:{context_id}"
    elif context_ns == "CL":
        context_id = f"CL:{context_id}"
    return context_ns, context_id
