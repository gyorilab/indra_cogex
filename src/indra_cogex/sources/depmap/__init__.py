"""Process DepMap, a resource for gene-gene dependencies in cancer cell lines."""

import logging
import pickle
import tqdm
from collections import defaultdict

import click
import pystow
from indra.databases import hgnc_client

from indra_cogex.representation import Node, Relation
from indra_cogex.sources.processor import Processor


__all__ = [
    "DepmapProcessor",
]

logger = logging.getLogger(__name__)

SUBMODULE = pystow.module("indra", "cogex", "depmap")

DEPMAP_SIGS = pystow.join('depmap_analysis', 'depmap', '21q2',
                          name='dep_stouffer_signif.pkl')


def load_sigs():
    with open(DEPMAP_SIGS, 'rb') as f:
        df = pickle.load(f)
    sig_by_gene = defaultdict(dict)
    for row in tqdm.tqdm(df.itertuples(), total=len(df),
                         desc='Processing DepMap significant pairs'):
        a, b = sorted(row.Index)
        a_hgnc_id = hgnc_client.get_current_hgnc_id(a)
        b_hgnc_id = hgnc_client.get_current_hgnc_id(b)
        if a_hgnc_id is None or b_hgnc_id is None:
            continue
        if isinstance(a_hgnc_id, list):
            a_hgnc_id = a_hgnc_id[0]
        if isinstance(b_hgnc_id, list):
            b_hgnc_id = b_hgnc_id[0]
        a_current = hgnc_client.get_hgnc_name(a_hgnc_id)
        b_current = hgnc_client.get_hgnc_name(b_hgnc_id)
        sig_by_gene[(a_current, a_hgnc_id)][(b_current, b_hgnc_id)] = row.logp
    sigs_by_gene = dict(sig_by_gene)
    return sigs_by_gene


class DepmapProcessor(Processor):
    """Processor for the DepMap dataset."""

    name = "depmap"
    node_types = ["BioEntity"]
    depmap_relation = "codependent_with"

    def __init__(self):
        """Initialize the DisGeNet processor."""
        self.sigs_by_gene = load_sigs()

    def get_nodes(self):  # noqa:D102
        all_genes = set(self.sigs_by_gene)
        for genes in self.sigs_by_gene.values():
            all_genes |= set(genes)

        for gene_name, hgnc_id in all_genes:
            yield Node(db_ns="HGNC", db_id=hgnc_id, labels=["BioEntity"],
                       data={'name': gene_name})

    def get_relations(self):  # noqa:D102
        for (a, a_hgnc_id), genes in \
                tqdm.tqdm(self.sigs_by_gene.items(),
                          desc='Processing DepMap into relations'):
            for (b, b_hgnc_id), logp in genes.items():
                yield Relation(
                    source_ns="HGNC",
                    source_id=a_hgnc_id,
                    target_ns="HGNC",
                    target_id=b_hgnc_id,
                    rel_type=self.depmap_relation,
                    data={"logp": logp},
                )
