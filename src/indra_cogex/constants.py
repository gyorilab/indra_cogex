from typing import Set

from indra.resources import load_resource_json

__all__ = [
    "DATABASES",
    "READERS",
]

_source_info = load_resource_json("source_info.json")

# DATABASES = {"biogrid", "hprd", "signor", "phosphoelm", "signor", "biopax"}
DATABASES: Set[str] = {
    key
    for key, value in _source_info.items()
    if value["type"] == "database"
}

READERS: Set[str] = {
    key
    for key, value in _source_info.items()
    if value["type"] == "reader"
}
