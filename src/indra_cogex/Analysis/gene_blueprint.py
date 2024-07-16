"""Gene-centric analysis blueprint."""

from typing import Dict, List, Mapping, Tuple
import pandas as pd
from indra.databases import hgnc_client
from indra_cogex.client.enrichment.continuous import (
    get_human_scores,
    get_mouse_scores,
    get_rat_scores,
    indra_downstream_gsea,
    indra_upstream_gsea,
    phenotype_gsea,
    reactome_gsea,
    wikipathways_gsea,
    go_gsea
)

from indra_cogex.client.enrichment.discrete import (
    go_ora,
    indra_downstream_ora,
    indra_upstream_ora,
    phenotype_ora,
    reactome_ora,
    wikipathways_ora,
)

from ...client.enrichment.signed import reverse_casual_reasoning


def parse_genes_field(s: str) -> Tuple[Dict[str, str], List[str]]:
    """Parse a gene field string."""
    records = {
        record.strip().strip('"').strip("'").strip()
        for line in s.strip().lstrip("[").rstrip("]").split()
        if line
        for record in line.strip().split(",")
        if record.strip()
    }
    hgnc_ids = []
    errors = []
    for entry in records:
        if entry.lower().startswith("hgnc:"):
            hgnc_ids.append(entry.lower().replace("hgnc:", "", 1))
        elif entry.isnumeric():
            hgnc_ids.append(entry)
        else:  # probably a symbol
            hgnc_id = hgnc_client.get_current_hgnc_id(entry)
            if hgnc_id:
                hgnc_ids.append(hgnc_id)
            else:
                errors.append(entry)
    genes = {hgnc_id: hgnc_client.get_hgnc_name(hgnc_id) for hgnc_id in hgnc_ids}
    return genes, errors

"""
"""

def discrete_analysis(client, genes: str, method: str, alpha: float, keep_insignificant: bool,
                      minimum_evidence_count: int, minimum_belief: float):

    """Render the home page."""
    genes, errors = parse_genes_field(genes)
    gene_set = set(genes)

    go_results = go_ora(
        client, gene_set, method=method, alpha=alpha, keep_insignificant=keep_insignificant
    )
    wikipathways_results = wikipathways_ora(
        client, gene_set, method=method, alpha=alpha, keep_insignificant=keep_insignificant
    )
    reactome_results = reactome_ora(
        client, gene_set, method=method, alpha=alpha, keep_insignificant=keep_insignificant
    )
    phenotype_results = phenotype_ora(
        gene_set, client=client, method=method, alpha=alpha, keep_insignificant=keep_insignificant
    )

    indra_upstream_results = indra_upstream_ora(
        client, gene_set, method=method, alpha=alpha, keep_insignificant=keep_insignificant,
        minimum_evidence_count=minimum_evidence_count, minimum_belief=minimum_belief
    )
    indra_downstream_results = indra_downstream_ora(
        client, gene_set, method=method, alpha=alpha, keep_insignificant=keep_insignificant,
        minimum_evidence_count=minimum_evidence_count, minimum_belief=minimum_belief
    )

    return {
        "go_results": go_results,
        "wikipathways_results": wikipathways_results,
        "reactome_results": reactome_results,
        "phenotype_results": phenotype_results,
        "indra_upstream_results": indra_upstream_results,
        "indra_downstream_results": indra_downstream_results,
        "errors": errors
    }

    def signed_analysis(client, positive_genes: str, negative_genes: str, alpha: float,
                        keep_insignificant: bool, minimum_evidence_count: int, minimum_belief: float):
    """Render the signed gene set enrichment analysis form."""
    positive_genes, positive_errors = parse_genes_field(positive_genes)
    negative_genes, negative_errors = parse_genes_field(negative_genes)

    results = reverse_causal_reasoning(
        client=client,
        positive_hgnc_ids=positive_genes,
        negative_hgnc_ids=negative_genes,
        alpha=alpha,
        keep_insignificant=keep_insignificant,
        minimum_evidence_count=minimum_evidence_count,
        minimum_belief=minimum_belief,
    )

    return {
        "results": results,
        "positive_errors": positive_errors,
        "negative_errors": negative_errors
    }

@gene_blueprint.route("/continuous", methods=["GET", "POST"])
def continuous_analysis():
    """Render the continuous analysis form."""
    form = ContinuousForm()
    form.file.description = """\
    Make sure the uploaded file contains at least two columns: one with gene names and 
    one with the values of the ranking metric. The first row od the file should contain 
    the column names."""
    if form.validate_on_submit():
        scores = form.get_scores()
        source = form.source.data
        alpha = form.alpha.data
        permutations = form.permutations.data
        keep_insignificant = form.keep_insignificant.data
        if source == "go":
            results = go_gsea(
                client=client,
                scores=scores,
                permutation_num=permutations,
                alpha=alpha,
                keep_insignificant=keep_insignificant,
            )
        elif source == "wikipathways":
            results = wikipathways_gsea(
                client=client,
                scores=scores,
                permutation_num=permutations,
                alpha=alpha,
                keep_insignificant=keep_insignificant,
            )
        elif source == "reactome":
            results = reactome_gsea(
                client=client,
                scores=scores,
                permutation_num=permutations,
                alpha=alpha,
                keep_insignificant=keep_insignificant,
            )
        elif source == "phenotype":
            results = phenotype_gsea(
                client=client,
                scores=scores,
                permutation_num=permutations,
                alpha=alpha,
                keep_insignificant=keep_insignificant,
            )
        elif source == "indra-upstream":
            results = indra_upstream_gsea(
                client=client,
                scores=scores,
                permutation_num=permutations,
                alpha=alpha,
                keep_insignificant=keep_insignificant,
                minimum_evidence_count=form.minimum_evidence.data,
                minimum_belief=form.minimum_belief.data,
            )
        elif source == "indra-downstream":
            results = indra_downstream_gsea(
                client=client,
                scores=scores,
                permutation_num=permutations,
                alpha=alpha,
                keep_insignificant=keep_insignificant,
                minimum_evidence_count=form.minimum_evidence.data,
                minimum_belief=form.minimum_belief.data,
            )
        else:
            raise ValueError(f"Unknown source: {source}")

        return flask.render_template(
            "gene_analysis/continuous_results.html",
            source=source,
            results=results,
        )
    return flask.render_template(
        "gene_analysis/continuous_form.html",
        form=form,
    )
