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
from indra_cogex.analysis import (
    metabolite_analysis,
    gene_analysis,
    gene_continuous_analysis_example_data,
    source_targets_explanation
)
from .helpers import ParseError, get_docstring, parse_json, process_result

logger = logging.getLogger(__name__)

__all__ = [
    "gene_expression_ns",
    "go_terms_ns",
    "clinical_trials_ns",
    "biological_pathways_ns",
    "drug_side_effects_ns",
    "ontology_ns",
    "literature_metadata_ns",
    "statements_ns",
    "drug_targets_ns",
    "cell_markers_ns",
    "disease_phenotypes_ns",
    "gene_disease_variant_ns",
    "research_project_output_ns",
    "gene_domains_ns",
    "phenotype_variant_ns",
    "drug_indications_ns",
    "gene_codependence_ns",
    "enzyme_activity_ns",
    "cell_line_properties_ns",
    "analysis_ns",
    "subnetwork_ns"
]

# Define category descriptions
CATEGORY_DESCRIPTIONS = {
    'gene_expression': "Query and validate gene expression patterns across different tissues",
    'go_terms': "Access and verify Gene Ontology term associations for genes",
    'clinical_trials': "Retrieve information about clinical trials and their relationships with drugs and diseases",
    'biological_pathways': "Explore relationships between genes and their biological pathways, including pathway "
                           "sharing analysis",
    'drug_side_effects': "Query and validate drug side effects and their associations",
    'ontology': "Navigate hierarchical relationships in biological and medical ontologies",
    'literature_metadata': "Access and search scientific literature, journal, and publisher metadata",
    'statements': "Retrieve and analyze evidence statements and their associated metadata",
    'drug_targets': "Explore and validate relationships between drugs and their molecular targets",
    'cell_markers': "Query relationships between cell types and their molecular markers",
    'disease_phenotypes': "Investigate associations between diseases and their phenotypic manifestations",
    'gene_disease_variant': "Analyze interconnected relationships between genes, diseases, and their genetic variants",
    'project_research_outputs': "Query bidirectional relationships between NIH research projects and their associated "
                                "outputs (publications, clinical trials, patents)",
    'gene_domains': "Query and validate protein domain associations for genes",
    'phenotype_variant': "Explore relationships between genetic variants and phenotypes from GWAS studies",
    'drug_indications': "Access information about therapeutic uses and indications for drugs",
    'gene_codependence': "Analyze genetic dependencies and interactions between genes",
    'enzyme_activity': "Query enzymatic activities associated with genes and proteins",
    'cell_line_properties': "Explore cell line characteristics including mutations, copy number alterations, "
                            "and drug sensitivity",
    'subnetwork': "Explore biological subnetwork relationships and pathways",
    'analysis': "Perform statistical and biological data analysis"
}

# Define all namespaces
gene_expression_ns = Namespace("Gene Expression Queries", CATEGORY_DESCRIPTIONS['gene_expression'], path="/api")
go_terms_ns = Namespace("GO Terms Queries", CATEGORY_DESCRIPTIONS['go_terms'], path="/api")
clinical_trials_ns = Namespace("Clinical Trials Queries", CATEGORY_DESCRIPTIONS['clinical_trials'], path="/api")
biological_pathways_ns = Namespace("Biological Pathways Queries", CATEGORY_DESCRIPTIONS['biological_pathways'],
                                   path="/api")
drug_side_effects_ns = Namespace("Drug Side Effects Queries", CATEGORY_DESCRIPTIONS['drug_side_effects'], path="/api")
ontology_ns = Namespace("Ontology Queries", CATEGORY_DESCRIPTIONS['ontology'], path="/api")
literature_metadata_ns = Namespace("Literature Metadata Queries", CATEGORY_DESCRIPTIONS['literature_metadata'],
                                   path="/api")
statements_ns = Namespace("Statements Queries", CATEGORY_DESCRIPTIONS['statements'], path="/api")
drug_targets_ns = Namespace("Drug Targets Queries", CATEGORY_DESCRIPTIONS['drug_targets'], path="/api")
cell_markers_ns = Namespace("Cell Markers Queries", CATEGORY_DESCRIPTIONS['cell_markers'], path="/api")
disease_phenotypes_ns = Namespace("Disease-Phenotypes Association Queries", CATEGORY_DESCRIPTIONS['disease_phenotypes'],
                                  path="/api")
gene_disease_variant_ns = Namespace("Gene-Disease-Variant Association Queries",
                                    CATEGORY_DESCRIPTIONS['gene_disease_variant'], path="/api")
research_project_output_ns = Namespace("Project-Research Queries", CATEGORY_DESCRIPTIONS['project_research_outputs'],
                                       path="/api")
gene_domains_ns = Namespace("Gene Domain Queries", CATEGORY_DESCRIPTIONS['gene_domains'], path="/api")
phenotype_variant_ns = Namespace("Phenotype-Variant Association Queries", CATEGORY_DESCRIPTIONS['phenotype_variant'],
                                 path="/api")
drug_indications_ns = Namespace("Drug Indication Queries", CATEGORY_DESCRIPTIONS['drug_indications'], path="/api")
gene_codependence_ns = Namespace("Gene Codependence Queries", CATEGORY_DESCRIPTIONS['gene_codependence'], path="/api")
enzyme_activity_ns = Namespace("Enzyme Activity Queries", CATEGORY_DESCRIPTIONS['enzyme_activity'], path="/api")
cell_line_properties_ns = Namespace("Cell Line Property Queries", CATEGORY_DESCRIPTIONS['cell_line_properties'],
                                    path="/api")
analysis_ns = Namespace("Analysis Queries", CATEGORY_DESCRIPTIONS['analysis'], path="/api")
subnetwork_ns = Namespace("Subnetwork Queries", CATEGORY_DESCRIPTIONS['subnetwork'], path="/api")


def get_example_data():
    """Get example data for gene continuous analysis."""
    reader = csv.reader(gene_continuous_analysis_example_data.open())
    _ = next(reader)  # Skip header
    names, log_fold_changes = zip(*reader)
    return names, [float(n) for n in log_fold_changes]


continuous_analysis_example_names, continuous_analysis_example_data = get_example_data()

FUNCTION_CATEGORIES = {
    'gene_expression': {
        'namespace': gene_expression_ns,
        'functions': [
            "get_genes_in_tissue",
            "get_tissues_for_gene",
            "is_gene_in_tissue"
        ]
    },
    'go_terms': {
        'namespace': go_terms_ns,
        'functions': [
            "get_go_terms_for_gene",
            "get_genes_for_go_term",
            "is_go_term_for_gene"
        ]
    },
    'clinical_trials': {
        'namespace': clinical_trials_ns,
        'functions': [
            "get_trials_for_drug",
            "get_trials_for_disease",
            "get_drugs_for_trial",
            "get_diseases_for_trial"
        ]
    },
    'biological_pathways': {
        'namespace': biological_pathways_ns,
        'functions': [
            "get_pathways_for_gene",
            "get_shared_pathways_for_genes",
            "get_genes_for_pathway",
            "is_gene_in_pathway"
        ]
    },
    'drug_side_effects': {
        'namespace': drug_side_effects_ns,
        'functions': [
            "get_side_effects_for_drug",
            "get_drugs_for_side_effect",
            "is_side_effect_for_drug"
        ]
    },
    'ontology': {
        'namespace': ontology_ns,
        'functions': [
            "get_ontology_child_terms",
            "get_ontology_parent_terms",
            "isa_or_partof"
        ]
    },
    'literature_metadata': {
        'namespace': literature_metadata_ns,
        'functions': [
            "get_pmids_for_mesh",
            "get_mesh_ids_for_pmid",
            "get_mesh_ids_for_pmids",
            "get_publisher_for_journal",
            "get_journals_for_publisher",
            "is_journal_published_by",
            "get_journal_for_publication",
            "get_publications_for_journal",
            "is_published_in_journal"
        ]
    },
    'statements': {
        'namespace': statements_ns,
        'functions': [
            "get_evidences_for_mesh",
            "get_evidences_for_stmt_hash",
            "get_evidences_for_stmt_hashes",
            "get_stmts_for_paper",
            "get_stmts_for_pmids",
            "get_stmts_for_mesh",
            "get_stmts_meta_for_stmt_hashes",
            "get_stmts_for_stmt_hashes",
            "get_statements",
            "get_mesh_annotated_evidence",
            "get_network"
        ]
    },
    'drug_targets': {
        'namespace': drug_targets_ns,
        'functions': [
            "get_drugs_for_target",
            "get_drugs_for_targets",
            "get_targets_for_drug",
            "get_targets_for_drugs",
            "is_drug_target"
        ]
    },
    'cell_markers': {
        'namespace': cell_markers_ns,
        'functions': [
            "get_markers_for_cell_type",
            "get_cell_types_for_marker",
            "is_marker_for_cell_type"
        ]
    },
    'disease_phenotypes': {
        'namespace': disease_phenotypes_ns,
        'functions': [
            "get_phenotypes_for_disease",
            "get_diseases_for_phenotype",
            "has_phenotype",
            "get_genes_for_phenotype",
            "get_phenotypes_for_gene",
            "has_phenotype_gene"
        ]
    },
    'gene_disease_variant': {
        'namespace': gene_disease_variant_ns,
        'functions': [
            "get_diseases_for_gene",
            "get_genes_for_disease",
            "has_gene_disease_association",
            "get_diseases_for_variant",
            "get_variants_for_disease",
            "has_variant_disease_association",
            "get_genes_for_variant",
            "get_variants_for_gene",
            "has_variant_gene_association"
        ]
    },
    'research_project_output': {
        'namespace': research_project_output_ns,
        'functions': [
            "get_publications_for_project",
            "get_clinical_trials_for_project",
            "get_patents_for_project",
            "get_projects_for_publication",
            "get_projects_for_clinical_trial",
            "get_projects_for_patent"
        ]
    },
    'gene_domains': {
        'namespace': gene_domains_ns,
        'functions': [
            "get_domains_for_gene",
            "get_genes_for_domain",
            "gene_has_domain"
        ]
    },
    'phenotype_variant': {
        'namespace': phenotype_variant_ns,
        'functions': [
            "get_phenotypes_for_variant_gwas",
            "get_variants_for_phenotype_gwas",
            "has_variant_phenotype_association"
        ]
    },
    'drug_indications': {
        'namespace': drug_indications_ns,
        'functions': [
            "get_indications_for_drug",
            "get_drugs_for_indication",
            "drug_has_indication"
        ]
    },
    'gene_codependence': {
        'namespace': gene_codependence_ns,
        'functions': [
            "get_codependents_for_gene",
            "gene_has_codependency"
        ]
    },
    'enzyme_activity': {
        'namespace': enzyme_activity_ns,
        'functions': [
            "get_enzyme_activities_for_gene",
            "get_genes_for_enzyme_activity",
            "has_enzyme_activity"
        ]
    },
    'cell_line_properties': {
        'namespace': cell_line_properties_ns,
        'functions': [
            "get_cell_lines_with_mutation",
            "get_mutated_genes_in_cell_line",
            "is_gene_mutated_in_cell_line",
            "get_cell_lines_with_cna",
            "get_cna_genes_in_cell_line",
            "has_cna_in_cell_line",
            "get_drugs_for_sensitive_cell_line",
            "get_sensitive_cell_lines_for_drug",
            "is_cell_line_sensitive_to_drug"
        ]
    },
    'analysis': {
        'namespace': analysis_ns,
        'functions': [
            "discrete_analysis",
            "signed_analysis",
            "continuous_analysis",
            "metabolite_discrete_analysis",
            "kinase_analysis",
            "explain_downstream",
        ]
    },
    'subnetwork': {
        'namespace': subnetwork_ns,
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
    "gene": {
        "get_enzyme_activities_for_gene": fields.List(fields.String, example=["hgnc", "10007"]),
        "default": fields.List(fields.String, example=["HGNC", "9896"])
    },
    "go_term": fields.List(fields.String, example=["GO", "GO:0000978"]),
    "drug": {
        "get_sensitive_cell_lines_for_drug": fields.List(fields.String, example=["mesh", "C586365"]),
        "default": fields.List(fields.String, example=["CHEBI", "CHEBI:27690"])
    },
    "drugs": fields.List(
        fields.List(fields.String),
        example=[["CHEBI", "CHEBI:27690"], ["CHEBI", "CHEBI:114785"]]
    ),
    "disease": fields.List(fields.String, example=["doid", "0040093"]),
    "trial": {
        "get_drugs_for_trial": fields.List(fields.String, example=["CLINICALTRIALS", "NCT00000114"]),
        "default": fields.List(fields.String, example=["CLINICALTRIALS", "NCT00201240"])
    },
    "genes": fields.List(
        fields.List(fields.String),
        example=[["HGNC", "1097"], ["HGNC", "6407"]]
    ),
    "pathway": fields.List(fields.String, example=["WIKIPATHWAYS", "WP5037"]),
    "side_effect": fields.List(fields.String, example=["UMLS", "C3267206"]),
    "term": fields.List(fields.String, example=["MESH", "D007855"]),
    "parent": fields.List(fields.String, example=["MESH", "D007855"]),
    "mesh_term": {
        "get_mesh_annotated_evidence": fields.List(
            fields.String, example=["MESH", "D012878"]
        ),
        "default": fields.List(fields.String, example=["MESH", "D015002"])
    },
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
    "stmt_hashes": {
        "get_mesh_annotated_evidence": fields.List(
            fields.String, example=[
                "9864896957797950", "20136431766023466", "-18896592574172325"
            ]
        ),
        "default": fields.List(fields.String,
                               example=["12198579805553967", "30651649296901235"])
    },
    "rel_type": fields.String(example="Phosphorylation"),
    "rel_types": fields.List(fields.String, example=["Phosphorylation", "Activation"]),
    "agent_name": fields.String(example="MEK"),
    "agent": fields.String(example="MEK"),
    "other_agent": fields.String(example="ERK"),
    "agent_role": fields.String(example="subject"),
    "other_role": fields.String(example="object"),
    "stmt_source": fields.String(example="reach"),
    "stmt_sources": fields.List(fields.String, example=["reach", "sparser"]),
    "mesh_terms": fields.List(fields.String, example=None),
    "include_db_evidence": fields.Boolean(example=True),
    "cell_line": fields.List(fields.String, example=["CCLE", "HEL_HAEMATOPOIETIC_AND_LYMPHOID_TISSUE"]),
    "target": fields.List(fields.String, example=["HGNC", "6840"]),
    "targets": {
        "explain_downstream": fields.List(fields.String, example=["TP53", "PARP1", "RAD51", "CHEK2"]),
        "default": fields.List(
            fields.List(fields.String),
            example=[["HGNC", "6840"], ["HGNC", "1097"]]
        )
    },
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
    "background_gene_list": fields.List(fields.String, example=[]),
    "phosphosite_list": fields.List(fields.String, example=[
        "RPS6KA1-S363", "RPS3-T42", "RPS6KA3-Y529",
        "RPS6KB1-S434", "RPS6-S244", "RPS6-S236",
        "RPA2-S29", "RPS6KB1-T412", "RNF8-T198",
        "ROCK2-Y722", "BDKRB2-Y177", "BECN1-Y333"
    ]),
    "background": fields.List(fields.String, example=[]),
    # Examples for positive_genes and negative_genes for /gene/signed
    "positive_genes": fields.List(fields.String, example=EXAMPLE_POSITIVE_HGNC_IDS),
    "negative_genes": fields.List(fields.String, example=EXAMPLE_NEGATIVE_HGNC_IDS),
    "gene_names": fields.List(fields.String, example=continuous_analysis_example_names),
    "target_id": fields.String(example="hgnc:646"),
    "is_downstream": fields.Boolean(example=False),
    "minimum_evidence": fields.Float(example=2),
    "log_fold_change": fields.List(fields.Float, example=continuous_analysis_example_data),
    "species": fields.String(example="human"),
    "permutations": fields.Integer(example=100),
    "source": {
        "explain_downstream": fields.String(example="BRCA1"),
        "default": fields.String(example="go")
    },
    "indra_path_analysis": fields.Boolean(example=False),
    # For soure-targets analysis
    "id_type": fields.String(example="hgnc.symbol"),
    # Cell marker
    "cell_type": fields.List(fields.String, example=["cl", "0000020"]),
    "marker": fields.List(fields.String, example=["hgnc", "11337"]),
    # Pubmed
    "publication": fields.List(fields.String, example=["pubmed", "11818301"]),
    "journal": fields.List(fields.String, example=["nlm", "100972832"]),
    # Disgenet
    "variant": {
        "get_phenotypes_for_variant_gwas": fields.List(fields.String, example=["dbsnp", "rs13015548"]),
        "default": fields.List(fields.String, example=["dbsnp", "rs9994441"])
    },
    # Wikidata
    "publisher": fields.List(fields.String, example=["isni", "0000000031304729"]),
    # NIH Reporter
    "project": {
        "get_patents_for_project": fields.List(fields.String, example=["nihreporter.project", "2106676"]),
        "default": fields.List(fields.String, example=["nihreporter.project", "6439077"])
    },

    "patent": fields.List(fields.String, example=["google.patent", "US5939275"]),
    # HPOA
    "phenotype": {
        "get_genes_for_phenotype": fields.List(fields.String, example=["MESH", "D009264"]),
        "get_variants_for_phenotype_gwas": fields.List(fields.String, example=["mesh", "D001827"]),
        "default": fields.List(fields.String, example=["hp", "0003138"])
    },
    # For InterPro
    "domain": fields.List(fields.String, example=["interpro", "IPR006047"]),
    # For DepMap codependency
    "gene1": fields.List(fields.String, example=["hgnc", "1234"]),
    "gene2": fields.List(fields.String, example=["hgnc", "5678"]),
    # For ChEMBL
    "molecule": fields.List(fields.String, example=["chebi", "10001"]),
    "indication": fields.List(fields.String, example=["mesh", "D002318"]),
    # For EC
    "enzyme": fields.List(fields.String, example=["ec-code", "3.4.21.105"]),
    "network_type": fields.String(example="paper"),
    "identifier": {
        "get_network": fields.Raw(example=["PUBMED", "23356518"]),
        "default": fields.Raw(example=["PUBMED", "23356518"])
    },
}

# Parameters to always skip in the examples and in the documentation
SKIP_GLOBAL = {
    "client",
    "return_evidence_counts",
    "kwargs",
    "subject_prefix",
    "object_prefix",
    "file_path",
    "remove_medscan",
}

# Parameters to skip for specific functions
SKIP_ARGUMENTS = {
    "get_stmts_for_stmt_hashes": {"return_evidence_counts", "evidence_map"},
    "get_statements": {"mesh_term", "include_child_terms"}
}

# This is the list of functions to be included
# To add a new function, make sure it is part of __all__ in the respective module or is
# listed explicitly below and properly documented in its docstring as well as having
# example values for its parameters in the examples_dict above.
module_functions = (
    [(queries, fn) for fn in queries.__all__] +
    [(subnetwork, fn) for fn in [
        "indra_subnetwork_relations",
        "indra_subnetwork_meta",
        "indra_mediated_subnetwork",
        "indra_subnetwork_tissue",
        "indra_subnetwork_go"]] +
    [(metabolite_analysis, fn) for fn in ["metabolite_discrete_analysis"]] +
    [(gene_analysis, fn) for fn in [
        "discrete_analysis",
        "signed_analysis",
        "continuous_analysis",
        "kinase_analysis"]] +
    [(source_targets_explanation, fn) for fn in ["explain_downstream"]]

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
            param_name: (
                # If param has function-specific examples
                examples_dict[param_name].get(func_name, examples_dict[param_name]["default"])
                if isinstance(examples_dict[param_name], dict)
                # If param has same example for all functions
                else examples_dict[param_name]
            )
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
