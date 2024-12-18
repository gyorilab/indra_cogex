from flask_restx import Api
from .bioentity.api import bioentity_ns

# Import and add namespaces after api is created
from .queries_web import (
    search_ns, validation_ns, evidence_ns, relationship_ns,
    disease_ns, publication_ns, ontology_ns, cell_line_ns,
    clinical_ns, mutation_ns, analysis_ns, network_ns
)

api = Api(
    title="INDRA CoGEx Query API",
    description="REST API for INDRA CoGEx queries",
    doc="/apidocs",
)


# Add all namespaces
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
api.add_namespace(analysis_ns)
api.add_namespace(network_ns)
api.add_namespace(bioentity_ns)

__all__ = ["api"]
