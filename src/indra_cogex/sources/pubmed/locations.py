import pystow

__all__ = ["resources", "raw_xml", "issn_nlm_map_path", "mesh_pmid_path",
           "pmid_year_types_path", "pmid_nlm_path", "journal_info_path"]

resources = pystow.module("indra", "cogex", "pubmed")
raw_xml = pystow.module("indra", "cogex", "pubmed", "raw_xml")
# For mapping ISSN to NLM (many-to-one mapping)
issn_nlm_map_path = resources.join(name="issn_nlm_map.csv.gz")
mesh_pmid_path = resources.join(name="mesh_pmids.csv.gz")
pmid_year_types_path = resources.join(name="pmid_years_types.tsv.gz")
pmid_nlm_path = resources.join(name="pmid_nlm.csv.gz")
journal_info_path = resources.join(name="journal_info.tsv.gz")
