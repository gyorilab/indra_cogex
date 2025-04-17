import logging
from pathlib import Path
from typing import Union

import pystow

from indra.config import get_config
from indra.util.statement_presentation import db_sources, reader_sources

try:
    from pusher import pusher
except ImportError:
    pusher = None


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
    "has_activity": "Enzyme Annotations",
    "published_by": "Journal-Publisher Associations",
}

INDRA_COGEX_WEB_LOCAL = (get_config("INDRA_COGEX_WEB_LOCAL") or "").lower() in {
    "t",
    "true",
}

APP_CACHE_MODULE = pystow.module("indra", "cogex", "app_cache")
APPS_DIR = Path(__file__).parent.absolute()
TEMPLATES_DIR = APPS_DIR / "templates"
STATIC_DIR = APPS_DIR / "static"
INDRA_COGEX_EXTENSION = "indra_cogex_client"
STATEMENT_CURATION_CACHE = "curation_cache"
SOURCE_BADGES_CSS = STATIC_DIR / "source_badges.css"
AGENT_NAME_CACHE = APP_CACHE_MODULE.join(name="search_agent_cache.pkl")
GUNICORN_CONFIG = APPS_DIR / "gunicorn.conf.py"

# Set VUE parameters
sources_dict = {
    "databases": [d for d in db_sources] + ["bel"],  # Fixme: temporary fix for BEL
    "reading": [r for r in reader_sources],
}

# Check for source_badges.css, and generate if it doesn't exist
if not SOURCE_BADGES_CSS.exists():
    logger.info("Generating source_badges.css")
    from indra.assemblers.html.assembler import generate_source_css

    generate_source_css(SOURCE_BADGES_CSS.absolute().as_posix())

    # Fixme: temporary fix for BEL
    # Open the generated file and append the BEL badge (a copy of the 'bel_lc' badge)
    from indra.assemblers.html.assembler import DEFAULT_SOURCE_COLORS
    from textwrap import dedent

    bel_color = DEFAULT_SOURCE_COLORS[0][1]["sources"]["bel_lc"]
    with open(SOURCE_BADGES_CSS, "a") as f:
        f.write(dedent(
            f"""\
            .source-bel {{
                background-color: {bel_color};
                color: black;
            }}"""))
    logger.info("source_badges.css generated with extra BEL badge.")


# Path to locally built package of indralab-vue
LOCAL_VUE: Union[str, bool] = get_config("LOCAL_VUE") or False

# Set up indralab-vue Vue components, either from local build or from S3
VUE_URL_ROOT = (get_config("VUE_URL_ROOT") or "").rstrip("/")
VUE_JS = "IndralabVue.umd.min.js"
VUE_CSS = "IndralabVue.css"
if LOCAL_VUE:
    VUE_SRC_JS: Union[bool, str] = False
    VUE_SRC_CSS: Union[bool, str] = False
else:
    if not VUE_URL_ROOT:
        logger.warning(
            "VUE_URL_ROOT not set in environment. Statement Vue components will "
            "not be available in the web app."
        )
        VUE_SRC_JS = False
        VUE_SRC_CSS = False
    else:
        VUE_SRC_JS = f"{VUE_URL_ROOT}/{VUE_JS}"
        VUE_SRC_CSS = f"{VUE_URL_ROOT}/{VUE_CSS}"

# Pusher parameters
pusher_app_id = get_config("CLARE_PUSHER_APP_ID")
pusher_key = get_config("CLARE_PUSHER_KEY")
pusher_secret = get_config("CLARE_PUSHER_SECRET")
pusher_cluster = get_config("CLARE_PUSHER_CLUSTER")

# Pusher app
if pusher is not None and pusher_app_id and pusher_key and pusher_secret and pusher_cluster:
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

PYOBO_RESOURCE_FILE_VERSIONS = {
    "mgi": "6.23",
    "rgd": "2024-05-31",
}
