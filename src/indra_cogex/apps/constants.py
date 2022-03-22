import os
from pathlib import Path

edge_labels = {
    "annotated_with": "MeSH Annotations",
    "associated_with": "GO Annotations",
    "has_citation": "Citations",
    "indra_rel": "Causal Relations",
    "expressed_in": "Gene Expressions",
    "copy_number_altered_in": "CNVs",
    "mutated_in": "Mutations",
    "xref": "Xrefs",
    "partof": "Part Of",
    "has_trial": "Disease Trials",
    "isa": "Subclasses",
    "haspart": "Has Part",
    "has_side_effect": "Side Effects",
    "tested_in": "Drug Trials",
    "sensitive_to": "Sensitivities",
    "has_indication": "Drug Indications",
}

INDRA_COGEX_WEB_LOCAL = os.environ.get("INDRA_COGEX_WEB_LOCAL", "").lower() in {
    "t",
    "true",
}

APPS_DIR = Path(__file__).parent.absolute()
TEMPLATES_DIR = APPS_DIR / "templates"
STATIC_DIR = APPS_DIR / "static"
INDRA_COGEX_EXTENSION = "indra_cogex_client"
