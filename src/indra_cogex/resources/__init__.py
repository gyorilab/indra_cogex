"""Resource files for INDRA CoGEx."""

from pathlib import Path
from typing import List

__all__ = [
    "ensure_disprot",
]

HERE = Path(__file__).parent.resolve()

#: URL for downloading most recent version of DisProt
DISPROT_URL = "https://www.disprot.org/api/search?release=current&show_ambiguous=true&show_obsolete=false&format=tsv&namespace=all&get_consensus=false"
DISPROT_PATH = HERE.joinpath("disprot_hgnc_ids.txt")
#: A set of genes that have *too* much information (e.g., TP53, IL-6)
#: that will be excluded
DISPROT_SKIP = {
    "1678",  # CD4 usually misgrounded to CD4 T cells
    "6018",
    "11998",
}


def ensure_disprot(refresh: bool = False) -> List[str]:
    if DISPROT_PATH.is_file() and not refresh:
        return DISPROT_PATH.read_text().splitlines()

    import pandas as pd
    from protmapper import uniprot_client

    df = pd.read_csv(DISPROT_URL, sep="\t", dtype=str)
    df = df[df["ncbi_taxon_id"] == "9606"]
    hgnc_ids = df["acc"].map(uniprot_client.get_hgnc_id).unique()
    hgnc_ids = [
        hgnc_id
        for hgnc_id in hgnc_ids
        if hgnc_id is not None and hgnc_id not in DISPROT_SKIP
    ]
    rv = sorted(hgnc_ids, key=int)
    DISPROT_PATH.write_text("\n".join(rv))
    return rv


def main():
    """Rebuild all resources"""
    ensure_disprot(refresh=True)


if __name__ == "__main__":
    main()
