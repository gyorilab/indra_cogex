from flask_restx import Api

from .bioentity.api import bioentity_ns
from .queries_web import query_ns

__all__ = [
    "api",
]

api = Api(
    title="INDRA CoGEx Query API",
    description="REST API for INDRA CoGEx queries",
    doc="/apidocs",
)

api.add_namespace(query_ns)
api.add_namespace(bioentity_ns)
