import logging
import csv
import gzip
from pathlib import Path

import tqdm

from indra.statements import stmt_from_json

from indra_cogex.sources.indra_db.locations import *
from indra_cogex.util import load_stmt_json_str

logger = logging.getLogger(__name__)

def get_latest_timestamp_prefix(bucket: str, prefix: str) -> str:
    import boto3
    from datetime import datetime
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    result = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/")
    timestamps = []
    for page in result:
        for cp in page.get("CommonPrefixes", []):
            folder_name = cp["Prefix"].split("/")[-2]
            try:
                timestamps.append(datetime.strptime(folder_name, "%Y%m%d-%H%M%S"))
            except ValueError:
                continue
    if not timestamps:
        raise ValueError("No valid timestamped folders found in S3.")

    latest_ts = max(timestamps).strftime("%Y%m%d-%H%M%S")
    return f"{prefix}{latest_ts}/"

def download_s3_file(bucket: str, s3_key: str, local_path: Path):
    import boto3
    s3 = boto3.client("s3")
    local_path.parent.mkdir(parents=True, exist_ok=True)
    s3.download_file(Bucket=bucket, Key=s3_key, Filename=str(local_path))
    logger.info(f"Downloaded s3://{bucket}/{s3_key} → {local_path}")


def export_assembly():
    bucket = "bigmech"
    s3_base_prefix = get_latest_timestamp_prefix(
        bucket, prefix="indra-db/dumps/cogex_files/")

    download_s3_file(bucket, f"{s3_base_prefix}processed_statements.tsv.gz",
                     processed_stmts_fname)
    download_s3_file(bucket, f"{s3_base_prefix}source_counts.pkl",
                     source_counts_fname)

    # Create grounded and unique dumps
    # from processed statement in readonly pipeline
    # Takes ~2.5 h
    if not grounded_stmts_fname.exists() or not unique_stmts_fname.exists():
        with (gzip.open(processed_stmts_fname, "rt") as fh,
              gzip.open(grounded_stmts_fname, "wt") as fh_out_gr,
              gzip.open(unique_stmts_fname, "wt") as fh_out_uniq):
            seen_hashes = set()
            reader = csv.reader(fh, delimiter="\t")
            writer_gr = csv.writer(fh_out_gr, delimiter="\t")
            writer_uniq = csv.writer(fh_out_uniq, delimiter="\t")
            for sh, stmt_json_str in tqdm.tqdm(
                reader,
                total=63928997,  # Note this is a hard-coded estimate
                desc="Gathering grounded and unique statements",
                unit_scale=True,
                unit="stmt"
            ):
                stmt = stmt_from_json(load_stmt_json_str(stmt_json_str))
                if len(stmt.real_agent_list()) < 2:
                    continue
                if all(
                    (set(agent.db_refs) - {"TEXT", "TEXT_NORM"})
                    for agent in stmt.real_agent_list()
                ):
                    writer_gr.writerow((sh, stmt_json_str))
                    if sh not in seen_hashes:
                        writer_uniq.writerow((sh, stmt_json_str))
                seen_hashes.add(sh)
    else:
        logger.info(
            f"Grounded and unique statements already dumped at "
            f"{grounded_stmts_fname.as_posix()} and "
            f"{unique_stmts_fname.as_posix()}, skipping..."
        )

    logger.info(f"Grounded and unique statement export completed")

    download_s3_file(bucket, f"{s3_base_prefix}belief_scores.pkl",
                     belief_scores_pkl_fname)

if __name__ == "__main__":
    export_assembly()