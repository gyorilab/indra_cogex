from flask_restx import Api, Namespace
from .bioentity.api import bioentity_ns

CATEGORY_DESCRIPTIONS = {
    'search': "Find and retrieve data across genes, tissues, drugs, and pathways",
    'validation': "Verify relationships and validate entity associations",
    'evidence': "Access evidence and statement data supporting relationships",
    'relationship': "Analyze connections between different biological entities",
    'disease': "Explore disease-phenotype-gene relationships",
    'publication': "Search and retrieve publication-related information",
    'cell_line': "Query cell line mutations and drug sensitivity data",
    'ontology': "Navigate ontology hierarchies and classifications",
    'clinical': "Access clinical trial and drug indication data",
    'mutation': "Analyze genetic mutations and their effects",
    'network': "Explore biological network relationships and pathways",
    'analysis': "Perform statistical and biological data analysis"
}

api = Api(
    title="INDRA CoGEx Query API",
    description="REST API for INDRA CoGEx queries",
    doc="/apidocs",
)

# Use namespace definitions to include descriptions
search_ns = Namespace("Search Operations", CATEGORY_DESCRIPTIONS['search'], path="/api")
validation_ns = Namespace("Validation Operations", CATEGORY_DESCRIPTIONS['validation'], path="/api")
evidence_ns = Namespace("Evidence Operations", CATEGORY_DESCRIPTIONS['evidence'], path="/api")
relationship_ns = Namespace("Relationship Operations", CATEGORY_DESCRIPTIONS['relationship'], path="/api")
disease_ns = Namespace("Disease Operations", CATEGORY_DESCRIPTIONS['disease'], path="/api")
publication_ns = Namespace("Publication Operations", CATEGORY_DESCRIPTIONS['publication'], path="/api")
cell_line_ns = Namespace("Cell Line Operations", CATEGORY_DESCRIPTIONS['cell_line'], path="/api")
ontology_ns = Namespace("Ontology Operations", CATEGORY_DESCRIPTIONS['ontology'], path="/api")
clinical_ns = Namespace("Clinical Operations", CATEGORY_DESCRIPTIONS['clinical'], path="/api")
mutation_ns = Namespace("Mutation Operations", CATEGORY_DESCRIPTIONS['mutation'], path="/api")
network_ns = Namespace("Network Operations", CATEGORY_DESCRIPTIONS['network'], path="/api")
analysis_ns = Namespace("Analysis Operations", CATEGORY_DESCRIPTIONS['analysis'], path="/api")


__all__ = [
    "api",
    "search_ns",
    "validation_ns",
    "evidence_ns",
    "relationship_ns",
    "disease_ns",
    "publication_ns",
    "ontology_ns",
    "cell_line_ns",
    "clinical_ns",
    "mutation_ns",
    "analysis_ns",
    "network_ns"
]

api.add_namespace(search_ns)
api.add_namespace(validation_ns)
api.add_namespace(evidence_ns)
api.add_namespace(relationship_ns)
api.add_namespace(disease_ns)
api.add_namespace(publication_ns)
api.add_namespace(cell_line_ns)
api.add_namespace(ontology_ns)
api.add_namespace(clinical_ns)
api.add_namespace(mutation_ns)
api.add_namespace(network_ns)
api.add_namespace(analysis_ns)
api.add_namespace(bioentity_ns)
