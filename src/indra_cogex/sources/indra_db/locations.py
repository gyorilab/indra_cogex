import pystow

__all__ = [
    "base_folder",
    "reading_text_content_fname",
    "text_refs_fname",
    "raw_stmts_fname",
    "drop_readings_fname",
    "reading_to_text_ref_map",
    "processed_stmts_fname",
    "grounded_stmts_fname",
    "unique_stmts_fname",
    "source_counts_fname",
    "refinements_fname",
    "belief_scores_pkl_fname",
    "refinement_cycles_fname",
    "DUMP_BUCKET",
    "DUMP_PREFIX",
]

base_folder = pystow.module("indra", "db")
reading_text_content_fname = base_folder.join(name="reading_text_content_meta.tsv.gz")
text_refs_fname = base_folder.join(name="text_refs_principal.tsv.gz")
raw_stmts_fname = base_folder.join(name="raw_statements.tsv.gz")
drop_readings_fname = base_folder.join(name="drop_readings.pkl")
reading_to_text_ref_map = base_folder.join(name="reading_to_text_ref_map.pkl")
processed_stmts_fname = base_folder.join(name="processed_statements.tsv.gz")
grounded_stmts_fname = base_folder.join(name="grounded_statements.tsv.gz")
unique_stmts_fname = base_folder.join(name="unique_statements.tsv.gz")
source_counts_fname = base_folder.join(name="source_counts.pkl")
refinements_fname = base_folder.join(name="refinements.tsv.gz")
belief_scores_pkl_fname = base_folder.join(name="belief_scores.pkl")
refinement_cycles_fname = base_folder.join(name="refinement_cycles.pkl")
DUMP_BUCKET = "bigmech"
DUMP_PREFIX = "indra-db/dumps/cogex_files/"
