import logging
from pathlib import Path
from typing import Union
from indra.util.statement_presentation import db_sources, reader_sources
from indra.config import get_config
from pusher import pusher


logger = logging.getLogger(__name__)


edge_labels = {
    "annotated_with": "MeSH Annotations",
    "associated_with": "GO Annotations",
    "has_citation": "Citations",
    "indra_rel": "Causal Relations",
    "expressed_in": "Gene Expressions",
    "copy_number_altered_in": "CNVs",
    "mutated_in": "Mutations",
    "partof": "Part Of",
    "has_trial": "Disease Trials",
    "isa": "Subclasses",
    "haspart": "Has Part",
    "has_side_effect": "Side Effects",
    "tested_in": "Drug Trials",
    "sensitive_to": "Sensitivities",
    "has_indication": "Drug Indications",
    "has_phenotype": "Disease Phenotypes",
    "phenotype_has_gene": "Phenotype Genes",
    "has_publication": "Project Publications",
    "has_clinical_trial": "Project Trials",
    "has_patent": "Project Patents",
    "has_marker": "Cell Markers",
    "has_domain": "Protein Domains",
    "gene_disease_association": "Gene Disease Associations",
    # Links Publications to Journals
    "published_in": "Journal Associations",
    "variant_disease_association": "Variant Disease Associations",
    "variant_gene_association": "Variant Gene Associations",
    "variant_phenotype_association": "Variant Phenotype Associations",
}

INDRA_COGEX_WEB_LOCAL = (get_config("INDRA_COGEX_WEB_LOCAL") or "").lower() in {
    "t",
    "true",
}

APPS_DIR = Path(__file__).parent.absolute()
TEMPLATES_DIR = APPS_DIR / "templates"
STATIC_DIR = APPS_DIR / "static"
INDRA_COGEX_EXTENSION = "indra_cogex_client"
STATEMENT_CURATION_CACHE = "curation_cache"
SOURCE_BADGES_CSS = STATIC_DIR / "source_badges.css"

# Set VUE parameters
sources_dict = {
    "databases": [d for d in db_sources],
    "reading": [r for r in reader_sources],
}

# Check for source_badges.css, and generate if it doesn't exist
if not SOURCE_BADGES_CSS.exists():
    logger.info("Generating source_badges.css")
    from indra.assemblers.html.assembler import generate_source_css

    generate_source_css(SOURCE_BADGES_CSS.absolute().as_posix())


# Path to locally built package of indralab-vue
LOCAL_VUE: Union[str, bool] = get_config("LOCAL_VUE") or False

# Set up indralab-vue Vue components, either from local build or from S3
VUE_DEPLOYMENT = get_config("VUE_DEPLOYMENT") or "latest"
VUE_BASE = f"https://bigmech.s3.amazonaws.com/indra-db/indralabvue-{VUE_DEPLOYMENT}/"
VUE_JS = "IndralabVue.umd.min.js"
VUE_CSS = "IndralabVue.css"
if LOCAL_VUE:
    VUE_SRC_JS: Union[bool, str] = False
    VUE_SRC_CSS: Union[bool, str] = False
else:
    VUE_SRC_JS = f"{VUE_BASE}{VUE_JS}"
    VUE_SRC_CSS = f"{VUE_BASE}{VUE_CSS}"

# Pusher parameters
pusher_app_id = get_config("CLARE_PUSHER_APP_ID")
pusher_key = get_config("CLARE_PUSHER_KEY")
pusher_secret = get_config("CLARE_PUSHER_SECRET")
pusher_cluster = get_config("CLARE_PUSHER_CLUSTER")

# Pusher app
if pusher_app_id and pusher_key and pusher_secret and pusher_cluster:
    pusher_app = pusher.Pusher(
        app_id=pusher_app_id,
        key=pusher_key,
        secret=pusher_secret,
        cluster=pusher_cluster,
        ssl=True,
    )
else:
    logger.warning(
        "Pusher app not configured. Please set the environment variables "
        "CLARE_PUSHER_APP_ID, CLARE_PUSHER_KEY, CLARE_PUSHER_SECRET, "
        "and CLARE_PUSHER_CLUSTER."
    )
    pusher_app = None
