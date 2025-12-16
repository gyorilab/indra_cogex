from flask_restx import Api
from .bioentity.api import bioentity_ns

# Import and add namespaces after api is created
from .queries_web import (
    gene_expression_ns,
    go_terms_ns,
    clinical_trials_ns,
    biological_pathways_ns,
    drug_side_effects_ns,
    ontology_ns,
    literature_metadata_ns,
    statements_ns,
    drug_targets_ns,
    cell_markers_ns,
    disease_phenotypes_ns,
    gene_disease_variant_ns,
    research_project_output_ns,
    gene_domains_ns,
    phenotype_variant_ns,
    drug_indications_ns,
    gene_codependence_ns,
    enzyme_activity_ns,
    cell_line_properties_ns,
    analysis_ns,
    subnetwork_ns
)

# Import MCP server
from .mcp_server import mcp


api = Api(
    title="INDRA CoGEx Query API",
    description="REST API for INDRA CoGEx queries",
    doc="/apidocs",
)


# Add all namespaces
api.add_namespace(gene_expression_ns)
api.add_namespace(go_terms_ns)
api.add_namespace(clinical_trials_ns)
api.add_namespace(biological_pathways_ns)
api.add_namespace(drug_side_effects_ns)
api.add_namespace(ontology_ns)
api.add_namespace(literature_metadata_ns)
api.add_namespace(statements_ns)
api.add_namespace(drug_targets_ns)
api.add_namespace(cell_markers_ns)
api.add_namespace(disease_phenotypes_ns)
api.add_namespace(gene_disease_variant_ns)
api.add_namespace(research_project_output_ns)
api.add_namespace(gene_domains_ns)
api.add_namespace(phenotype_variant_ns)
api.add_namespace(drug_indications_ns)
api.add_namespace(gene_codependence_ns)
api.add_namespace(enzyme_activity_ns)
api.add_namespace(cell_line_properties_ns)
api.add_namespace(analysis_ns)
api.add_namespace(subnetwork_ns)
api.add_namespace(bioentity_ns)

__all__ = ["api", "mcp"]
