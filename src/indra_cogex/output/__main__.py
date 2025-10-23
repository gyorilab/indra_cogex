import click
from datetime import datetime, UTC

from depmap_analysis.util.aws import (
    dump_pickle_to_s3,
    NETS_PREFIX,
    NETS_SOURCE_DATA_PREFIX_SUBDIR,
    SIF_PKL_NAME,
)

from indra_cogex.output.network_search import sif_with_logp


@click.command()
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
def main(date: str, skip_upload: bool):
    """Main function dumping the latest SIF and statement hash-mesh ID mappings"""
    sif_df = sif_with_logp(method="ingestion_files")

    # Save the results to the depmap-analysis bucket on S3
    prefix = f"{NETS_PREFIX}/{date}/{NETS_SOURCE_DATA_PREFIX_SUBDIR}/".replace("//", "/")
    if not skip_upload:
        dump_pickle_to_s3(
            pyobj=sif_df,
            name=SIF_PKL_NAME,
            prefix=prefix,
        )
    else:
        click.echo(f"Skipping upload to S3. Results are saved locally to {SIF_PKL_NAME}")
        sif_df.to_pickle(SIF_PKL_NAME)

if __name__ == "__main__":
    main()
