"""Process DepMap, a resource for gene-gene dependencies in cancer cell lines."""

import logging
from pathlib import Path
from typing import Dict, Union
from collections import defaultdict

import pandas as pd
import numpy as np
from scipy import stats
from indra.databases import hgnc_client
from depmap_analysis.util.statistics import get_z, get_logp, get_n

from indra_cogex.representation import Node, Relation
from indra_cogex.sources.processor import Processor

from indra_cogex.sources.depmap.download_data import (
    MITOCARTA_FILE,
    MODEL_INFO_FILE,
    RNAI_FILE,
    CRISPR_FILE,
    DEPMAP_RELEASE_MODULE,
    SUBMODULE,
    download_source_files,
)


__all__ = [
    "DepmapProcessor",
]

logger = logging.getLogger(__name__)

#: DepMap derived files
RNAI_CORRS = SUBMODULE.join(name='rnai_corrs.h5')
RNAI_SAMPL = SUBMODULE.join(name='rnai_n.h5')
RNAI_LOGP = SUBMODULE.join(name='rnai_logp.h5')
RNAI_Z_LOG = SUBMODULE.join(name='rnai_z_log.h5')
CRISPR_CORRS = DEPMAP_RELEASE_MODULE.join(name='crispr_correlations.h5')
CRISPR_SAMPL = DEPMAP_RELEASE_MODULE.join(name='crispr_n.h5')
CRISPR_LOGP = DEPMAP_RELEASE_MODULE.join(name='crispr_logp.h5')
CRISPR_Z_LOG = DEPMAP_RELEASE_MODULE.join(name='crispr_z_log.h5')

DEPMAP_SIGS_PKL_NAME = "dep_stouffer_signif.pkl"  # Output file used in node/edge generation
DEPMAP_SIGS = DEPMAP_RELEASE_MODULE.join(name=DEPMAP_SIGS_PKL_NAME)

CORRECTION_METHODS = {
    'bonferroni': 'bc_cutoff',
    'benjamini-hochberg': 'bh_crit_val',
    'benjamini-yekutieli': 'by_crit_val',
}

CORRECTION_METHOD = 'benjamini-yekutieli'


def ensure_source_files(force: bool = False):
    if force or not all([f.exists() for f in
                         [MITOCARTA_FILE, MODEL_INFO_FILE, RNAI_FILE, CRISPR_FILE]]):
        download_source_files(force=force)
    else:
        logger.info("All source files exist, skipping download.")


def get_corr(
    recalculate: bool,
    data_df: pd.DataFrame,
    filepath: Union[str, Path]
) -> pd.DataFrame:
    if not Path(filepath).name.endswith('.h5'):
        filepath = Path(filepath).with_suffix('.h5')
    filename = Path(filepath).name
    if recalculate or not filepath.exists():
        data_corr = data_df.corr()
        data_corr.to_hdf(str(filepath), filename.split('.')[0])
    else:
        data_corr = pd.read_hdf(str(filepath))
    return data_corr


def unstack_corrs(df: pd.DataFrame) -> pd.DataFrame:
    df_ut = df.where(np.triu(np.ones(df.shape), k=1).astype(np.bool))
    stacked = df_ut.stack(dropna=True)
    return stacked


def filt_mitocorrs(ser: pd.DataFrame, mitogenes: list[str]) -> pd.Series:
    filt_ix = []
    filt_vals = []
    for (ix0, ix1), logp in ser.iteritems():
        if ix0 in mitogenes and ix1 in mitogenes:
            continue
        filt_ix.append((ix0, ix1))
        filt_vals.append(logp)
    index = pd.MultiIndex.from_tuples(filt_ix, names=['geneA', 'geneB'])
    filt_ser = pd.Series(filt_vals, index=index)
    return filt_ser


def create_sig_df(recalculate: bool = True) -> pd.DataFrame:
    """

    Parameters
    ----------
    recalculate :

    Returns
    -------
    :
        A pandas DataFrame of significant pairs of genes.
    """
    if not recalculate and DEPMAP_SIGS.exists():
        return pd.read_pickle(DEPMAP_SIGS)

    # Ensure source files are downloaded
    # ensure_source_files()

    # Process Mitocarta data
    mitocarta = pd.read_excel(mitocarta_file, sheet_name=1)
    mitogenes = mitocarta.Symbol.to_list()

    # Process cell line info from DepMap
    cell_line_df = pd.read_csv(sample_info_file)
    cell_line_map = cell_line_df[['DepMap_ID', 'CCLE_Name']]
    cell_line_map.set_index('CCLE_Name', inplace=True)

    # Process RNAi data
    rnai_df = pd.read_csv(rnai_file, index_col=0)
    rnai_df = rnai_df.transpose()
    gene_cols = ['%s' % col.split(' ')[0] for col in rnai_df.columns]
    rnai_df.columns = gene_cols
    rnai_df = rnai_df.join(cell_line_map)
    rnai_df = rnai_df.set_index('DepMap_ID')
    # Drop duplicate columns
    rnai_df = rnai_df.loc[:, ~rnai_df.columns.duplicated()]

    rnai_corr = get_corr(recalculate, rnai_df, 'rnai_correlations')
    rnai_n = get_n(recalculate, rnai_df, SUBMODULE.join(name='rnai_n'))
    rnai_logp = get_logp(recalculate, rnai_n, rnai_corr, SUBMODULE.join(name='rnai_logp'))
    rnai_z = get_z(recalculate, rnai_logp, rnai_corr, SUBMODULE.join(name='rnai_z_log'))

    # Process CRISPR data
    crispr_df = pd.read_csv(crispr_file, index_col=0)
    gene_cols = ['%s' % col.split(' ')[0] for col in crispr_df.columns]
    crispr_df.columns = gene_cols
    # Drop any duplicate columns (shouldn't be any for CRISPR, but just in case)
    crispr_df = crispr_df.loc[:, ~crispr_df.columns.duplicated()]

    crispr_corr = get_corr(recalculate, crispr_df, 'crispr_correlations')
    crispr_n = get_n(recalculate, crispr_df, SUBMODULE.join(name='crispr_n'))
    crispr_logp = get_logp(recalculate, crispr_n, crispr_corr, SUBMODULE.join(name='crispr_logp'))
    crispr_z = get_z(recalculate, crispr_logp, crispr_corr, SUBMODULE.join(name='crispr_z_log'))

    # Combine z-scores
    dep_z = (crispr_z + rnai_z) / np.sqrt(2)
    dep_z = dep_z.dropna(axis=0, how='all').dropna(axis=1, how='all')

    dep_logp = pd.DataFrame(np.log(2) + stats.norm.logcdf(-dep_z.abs()),
                            index=dep_z.columns, columns=dep_z.columns)

    filename = 'dep_z'
    z_filepath = SUBMODULE.join(name='%s.h5' % filename)
    dep_z.to_hdf(z_filepath, filename)

    filename = 'dep_logp'
    logp_filepath = SUBMODULE.join(name='%s.h5' % filename)
    dep_logp.to_hdf(logp_filepath, filename)

    df_logp = dep_logp
    total_comps = np.triu(~df_logp.isna(), k=1).sum()
    mitogenes_in_df = set(df_logp.columns).intersection(set(mitogenes))
    mito_comps = len(mitogenes_in_df)**2
    num_comps = total_comps - mito_comps

    alpha = 0.05
    bc_thresh = np.log(alpha / num_comps)
    sig_no_corr = unstack_corrs(df_logp[df_logp < np.log(alpha)])
    filt_corrs = filt_mitocorrs(sig_no_corr, mitogenes=mitogenes)
    sig_sorted = filt_corrs.sort_values().to_frame('logp')
    sig_sorted['rank'] = sig_sorted.rank()
    sig_sorted['bc_cutoff'] = bc_thresh
    sig_sorted['bh_crit_val'] = np.log((sig_sorted['rank'] / num_comps) * alpha)
    cm = np.log(num_comps) + np.euler_gamma + (1 / (2 * num_comps))

    sig_sorted['by_crit_val'] = sig_sorted['bh_crit_val'] - np.log(cm)
    sig_sorted.to_pickle(DEPMAP_SIGS)
    return sig_sorted


def load_sigs(correction_method=CORRECTION_METHOD):
    # Load the significance data frame
    with open(DEPMAP_SIGS, 'rb') as f:
        df = pickle.load(f)

    # Apply correction method filter
    if correction_method is not None:
        crit_col = CORRECTION_METHODS[correction_method]
        df = df[df.logp < df[crit_col]]

    # Get the current HGNC IDs for the genes since
    # some are outdated and organize them by pairs
    sig_by_gene = defaultdict(dict)
    for row in tqdm.tqdm(df.itertuples(), total=len(df),
                         desc='Processing DepMap significant pairs'):
        # Note that we are sorting the genes here since
        # we will generate a single directed edge a->b
        # and this makes that process deterministic
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
        # Note that we have previously sorted a and b and
        # we are generating a single directed edge a->b here
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
