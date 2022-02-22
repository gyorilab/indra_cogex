"""Web constants."""

import os

__all__ = [
    "INDRA_COGEX_WEB_LOCAL",
]

INDRA_COGEX_WEB_LOCAL = os.environ.get("INDRA_COGEX_WEB_LOCAL")
