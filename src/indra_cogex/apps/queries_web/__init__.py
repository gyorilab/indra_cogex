# -*- coding: utf-8 -*-

"""An app wrapping the several modules of indra_cogex.

The endpoints are created dynamically based on the functions in the following modules:
- indra_cogex.client.queries
- indra_cogex.client.subnetwork
- indra_cogex.analysis.metabolite_analysis
- indra_cogex.analysis.gene_analysis
"""
import csv
import logging
from http import HTTPStatus
from inspect import isfunction, signature

from flask import jsonify, request
from flask_restx import Resource, abort, fields, Namespace

from indra_cogex.apps.proxies import client
from indra_cogex.client import queries, subnetwork
from indra_cogex.client.enrichment.mla import EXAMPLE_CHEBI_CURIES
from indra_cogex.client.enrichment.discrete import EXAMPLE_GENE_IDS
from indra_cogex.client.enrichment.signed import EXAMPLE_POSITIVE_HGNC_IDS, EXAMPLE_NEGATIVE_HGNC_IDS
from indra_cogex.analysis import metabolite_analysis, gene_analysis, gene_continuous_analysis_example_data

from .helpers import ParseError, get_docstring, parse_json, process_result

__all__ = [
    "api"
]

from ..rest_api import (
    api, search_ns, validation_ns, evidence_ns, relationship_ns,
    disease_ns, publication_ns, ontology_ns, cell_line_ns,
    clinical_ns, mutation_ns, analysis_ns, network_ns
)

logger = logging.getLogger(__name__)


def get_example_data():
    """Get example data for gene continuous analysis."""
    reader = csv.reader(gene_continuous_analysis_example_data.open())
    _ = next(reader)  # Skip header
    names, log_fold_changes = zip(*reader)
    return names, [float(n) for n in log_fold_changes]


continuous_analysis_example_names, continuous_analysis_example_data = get_example_data()


FUNCTION_CATEGORIES = {
    'search': {
        'namespace': search_ns,
        'functions': [
            "get_genes_in_tissue",
            "get_tissues_for_gene",
            "get_go_terms_for_gene",
            "get_genes_for_go_term",
            "get_trials_for_drug",
            "get_trials_for_disease",
            "get_drugs_for_trial",
            "get_diseases_for_trial",
            "get_pathways_for_gene",
            "get_genes_for_pathway",
            "get_shared_pathways_for_genes",
            "get_side_effects_for_drug",
            "get_drugs_for_side_effect",
            "get_markers_for_cell_type",
            "get_cell_types_for_marker"
        ]
    },
    'validation': {
        'namespace': validation_ns,
        'functions': [
            "is_gene_in_tissue",
            "is_go_term_for_gene",
            "is_gene_in_pathway",
            "is_side_effect_for_drug",
            "is_drug_target",
            "is_marker_for_cell_type",
            "is_journal_published_by",
            "is_published_in_journal",
            "is_gene_mutated",
            "is_gene_mutated_in_cell_line",
            "is_cell_line_sensitive_to_drug",
            "has_domain",
            "has_enzyme_activity",
            "has_indication",
            "has_variant_phenotype_association",
            "has_phenotype",
            "has_phenotype_gene",
            "has_gene_disease_association",
            "has_variant_disease_association",
            "has_variant_gene_association",
            "has_codependency",
            "has_cna_in_cell_line"
        ]
    },
    'evidence': {
        'namespace': evidence_ns,
        'functions': [
            "get_evidences_for_mesh",
            "get_evidences_for_stmt_hash",
            "get_evidences_for_stmt_hashes",
            "get_stmts_for_paper",
            "get_stmts_for_pmids",
            "get_stmts_for_mesh",
            "get_stmts_meta_for_stmt_hashes",
            "get_stmts_for_stmt_hashes",
            "get_statements"
        ]
    },
    'relationship': {
        'namespace': relationship_ns,
        'functions': [
            "get_drugs_for_target",
            "get_drugs_for_targets",
            "get_targets_for_drug",
            "get_targets_for_drugs",
            "get_genes_for_variant",
            "get_variants_for_gene",
            "get_codependents_for_gene"
        ]
    },
    'disease': {
        'namespace': disease_ns,
        'functions': [
            "get_phenotypes_for_disease",
            "get_diseases_for_phenotype",
            "get_genes_for_phenotype",
            "get_phenotypes_for_gene",
            "get_diseases_for_gene",
            "get_genes_for_disease",
            "get_diseases_for_variant",
            "get_variants_for_disease",
            "get_phenotypes_for_variant_gwas",
            "get_variants_for_phenotype_gwas"
        ]
    },
    'publication': {
        'namespace': publication_ns,
        'functions': [
            "get_pmids_for_mesh",
            "get_mesh_ids_for_pmid",
            "get_mesh_ids_for_pmids",
            "get_publisher_for_journal",
            "get_journals_for_publisher",
            "get_journal_for_publication",
            "get_publications_for_journal",
            "get_publications_for_project",
            "get_patents_for_project"
        ]
    },
    'cell_line': {
        'namespace': cell_line_ns,
        'functions': [
            "get_cell_lines_with_mutation",
            "get_mutated_genes_in_cell_line",
            "get_cell_lines_with_cna",
            "get_cna_genes_in_cell_line",
            "get_drugs_for_sensitive_cell_line",
            "get_sensitive_cell_lines_for_drug"
        ]
    },
    'ontology': {
        'namespace': ontology_ns,
        'functions': [
            "get_ontology_child_terms",
            "get_ontology_parent_terms",
            "isa_or_partof",
            "get_domains_for_gene",
            "get_genes_for_domain",
            "get_enzyme_activities_for_gene",
            "get_genes_for_enzyme_activity"
        ]
    },
    'clinical': {
        'namespace': clinical_ns,
        'functions': [
            "get_clinical_trials_for_project",
            "get_indications_for_molecule",
            "get_molecules_for_indication"
        ]
    },
    'mutation': {
        'namespace': mutation_ns,
        'functions': [
            "get_mutated_genes"
        ]
    },
    'analysis': {
        'namespace': analysis_ns,
        'functions': [
            "discrete_analysis",
            "signed_analysis",
            "continuous_analysis",
            "metabolite_discrete_analysis"
        ]
    },
    'network': {
        'namespace': network_ns,
        'functions': [
            "indra_subnetwork_relations",
            "indra_subnetwork_meta",
            "indra_subnetwork_tissue",
            "indra_subnetwork_go",
            "indra_mediated_subnetwork"
        ]
    }
}

examples_dict = {
    "tissue": fields.List(fields.String, example=["UBERON", "UBERON:0001162"]),
    "gene": fields.List(fields.String, example=["HGNC", "9896"]),
    "go_term": fields.List(fields.String, example=["GO", "GO:0000978"]),
    "drug": fields.List(fields.String, example=["CHEBI", "CHEBI:27690"]),
    "drugs": fields.List(
        fields.List(fields.String),
        example=[["CHEBI", "CHEBI:27690"], ["CHEBI", "CHEBI:114785"]]
    ),
    "disease": fields.List(fields.String, example=["MESH", "D007855"]),
    "trial": fields.List(fields.String, example=["CLINICALTRIALS", "NCT00000114"]),
    "genes": fields.List(
        fields.List(fields.String),
        example=[["HGNC", "1097"], ["HGNC", "6407"]]
    ),
    "pathway": fields.List(fields.String, example=["WIKIPATHWAYS", "WP5037"]),
    "side_effect": fields.List(fields.String, example=["UMLS", "C3267206"]),
    "term": fields.List(fields.String, example=["MESH", "D007855"]),
    "parent": fields.List(fields.String, example=["MESH", "D007855"]),
    "mesh_term": fields.List(fields.String, example=["MESH", "D015002"]),
    "pmid_term": fields.List(fields.String, example=["PUBMED", "34634383"]),
    "paper_term": fields.List(fields.String, example=["PUBMED", "23356518"]),
    "pmids": fields.List(fields.String, example=["20861832", "19503834"]),
    "mediated": fields.Boolean(example=False),
    "upstream_controllers": fields.Boolean(example=False),
    "downstream_targets": fields.Boolean(example=False),
    "include_child_terms": fields.Boolean(example=True),
    "return_source_counts": fields.Boolean(example=False),
    "order_by_ev_count": fields.Boolean(example=False),
    # NOTE: statement hashes are too large to be int for JavaScript
    "stmt_hash": fields.String(example="12198579805553967"),
    "stmt_hashes": fields.List(fields.String, example=["12198579805553967", "30651649296901235"]),
    "rel_type": fields.String(example="Phosphorylation"),
    "rel_types": fields.List(fields.String, example=["Phosphorylation", "Activation"]),
    "agent_name": fields.String(example="MEK"),
    "agent": fields.String(example="MEK"),
    "other_agent": fields.String(example="ERK"),
    "agent_role": fields.String(example="Subject"),
    "other_role": fields.String(example="Object"),
    "stmt_source": fields.String(example="reach"),
    "stmt_sources": fields.List(fields.String, example=["reach", "sparser"]),
    "include_db_evidence": fields.Boolean(example=True),
    "cell_line": fields.List(fields.String, example=["CCLE", "BT20_BREAST", "HEL_HAEMATOPOIETIC_AND_LYMPHOID_TISSUE"]),
    "target": fields.List(fields.String, example=["HGNC", "6840"]),
    "targets": fields.List(
        fields.List(fields.String),
        example=[["HGNC", "6840"], ["HGNC", "1097"]]
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
    # Metabolite analysis, and gene analysis examples (discrete, signed, continuous)
    "metabolites": fields.List(fields.String, example=EXAMPLE_CHEBI_CURIES),
    "method": fields.String(example="fdr_bh"),
    "alpha": fields.Float(example=0.05, min=0, max=1),
    "keep_insignificant": fields.Boolean(example=False),
    "minimum_evidence_count": fields.Integer(example=2),
    "minimum_belief": fields.Float(example=0.7, min=0, max=1),
    "ec_code": fields.String(example="3.2.1.4"),
    # Example for /gene/discrete
    "gene_list": fields.List(fields.String, example=EXAMPLE_GENE_IDS),
    # Examples for positive_genes and negative_genes for /gene/signed
    "positive_genes": fields.List(fields.String, example=EXAMPLE_POSITIVE_HGNC_IDS),
    "negative_genes": fields.List(fields.String, example=EXAMPLE_NEGATIVE_HGNC_IDS),
    "gene_names": fields.List(fields.String, example=continuous_analysis_example_names),
    "log_fold_change": fields.List(fields.Float, example=continuous_analysis_example_data),
    "species": fields.String(example="human"),
    "permutations": fields.Integer(example=100),
    "source": fields.String(example="go"),
    "indra_path_analysis": fields.Boolean(example=False),
    # Cell marker
    "cell_type": fields.List(fields.String, example=["cl", "0000020"]),
    "marker": fields.List(fields.String, example=["hgnc", "11337"]),
    # Pubmed
    "publication": fields.List(fields.String, example=["pubmed", "14334679"]),
    "journal": fields.List(fields.String, example=["nlm", "0000201"]),
    # Disgenet
    "variant": fields.List(fields.String, example=[["dbsnp", "rs9994441"], ["dbsnp", "rs13015548"]]),
    # Wikidata
    "publisher": fields.List(fields.String, example=["isni", "0000000031304729"]),
    # NIH Reporter
    "project": fields.List(fields.String, example=["nihreporter.project", "2106659"]),
    # HPOA
    "phenotype": fields.List(fields.String, example=[["hp", "0003138"], ["mesh", "D001827"]]),
    # For InterPro
    "domain": fields.List(fields.String, example=["interpro", "IPR006047"]),
    # For DepMap codependency
    "gene1": fields.List(fields.String, example=["hgnc", "1234"]),
    "gene2": fields.List(fields.String, example=["hgnc", "5678"]),
    # For ChEMBL
    "molecule": fields.List(fields.String, examples=[["chebi", "10001"], ["chembl.compound", "CHEMBL25"]]),
    "indication": fields.List(fields.String, example=["mesh", "D002318"]),
    # For EC
    "enzyme": fields.List(fields.String, example=["ec-code", "3.4.21.105"]),
}

# Parameters to always skip in the examples and in the documentation
SKIP_GLOBAL = {"client", "return_evidence_counts", "kwargs",
               "subject_prefix", "object_prefix", "file_path"}

# Parameters to skip for specific functions
SKIP_ARGUMENTS = {
    "get_stmts_for_stmt_hashes": {"return_evidence_counts", "evidence_map"},
    "get_evidences_for_stmt_hash": {"remove_medscan"},
    "get_evidences_for_stmt_hashes": {"remove_medscan"},
    "get_statements": {"mesh_term", "include_child_terms"}
}

# This is the list of functions to be included
# To add a new function, make sure it is part of __all__ in the respective module or is
# listed explicitly below and properly documented in its docstring as well as having
# example values for its parameters in the examples_dict above.
module_functions = (
    [(queries, fn) for fn in queries.__all__] +
    [(subnetwork, fn) for fn in ["indra_subnetwork_relations", "indra_subnetwork_meta", "indra_mediated_subnetwork",
                                 "indra_subnetwork_tissue", "indra_subnetwork_go"]] +
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

    # Find the appropriate namespace for this function
    target_ns = None
    for category, info in FUNCTION_CATEGORIES.items():
        if func_name in info['functions']:
            target_ns = info['namespace']
            break

    # If no category found, use search namespace as default
    if target_ns is None:
        target_ns = search_ns

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
    query_model = target_ns.model(
        model_name,
        {
            param_name: examples_dict[param_name]
            for param_name in param_names
            if param_name not in SKIP_GLOBAL
               and param_name not in SKIP_ARGUMENTS.get(func_name, [])
        },
    )


    @target_ns.expect(query_model)
    @target_ns.route(f"/{func_name}", doc={"summary": short_doc})
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
