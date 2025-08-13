import pickle
from pathlib import Path

import click
from datetime import datetime, UTC

from depmap_analysis.util.aws import (
    dump_pickle_to_s3,
    NETS_PREFIX,
    NETS_SOURCE_DATA_PREFIX_SUBDIR,
    SIF_PKL_NAME,
    STMT_HASH_MESH_PKL_NAME
)

from indra_cogex.client.neo4j_client import Neo4jClient
from indra_cogex.output.network_search import sif_with_logp, get_stmt_hash_to_mesh_map


@click.command()
@click.option("--limit", type=int, default=None, help="Limit the number of relations to process")
@click.option(
    "--date",
    type=str,
    default=datetime.now(UTC).strftime("%Y-%m-%d"),
    show_default=True,
    help="Date for the dump prefix")
@click.option(
    "--skip-upload",
    is_flag=True,
    default=False,
    help="Skip uploading the results to S3. Useful for local testing.",
)
def main(limit: int, date: str, skip_upload: bool):
    """Main function dumping the latest SIF and statement hash-mesh ID mappings"""
    client = Neo4jClient()
    sif_df = sif_with_logp(client, limit=limit)
    mesh_map = get_stmt_hash_to_mesh_map(client, limit=limit)

    # Save the results to the depmap-analysis bucket on S3
    prefix = f"{NETS_PREFIX}/{date}/{NETS_SOURCE_DATA_PREFIX_SUBDIR}/".replace("//", "/")
    if not skip_upload:
        dump_pickle_to_s3(
            pyobj=sif_df,
            name=SIF_PKL_NAME,
            prefix=prefix,
        )
        dump_pickle_to_s3(
            pyobj=mesh_map,
            name=STMT_HASH_MESH_PKL_NAME,
            prefix=prefix,
        )
    else:
        click.echo("Skipping upload to S3. Results are saved locally.")
        sif_df.to_pickle(SIF_PKL_NAME)
        Path(STMT_HASH_MESH_PKL_NAME).open("wb").write(
            pickle.dumps(mesh_map)
        )


if __name__ == "__main__":
    main()
