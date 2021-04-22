import click
import json
import os
from textwrap import dedent
from typing import Optional

from indra_cogex.neo4j_client import Neo4jClient
from indra_cogex.sources import BgeeProcessor, Processor

HERE = os.path.abspath(os.path.dirname(__file__))
DATA = os.path.abspath(os.path.join(HERE, os.pardir, ))


def dump_processor(
    client: Neo4jClient,
    processor: Processor,
    directory: Optional[str] = None,
):
    if directory is None:
        directory = processor.directory



@click.command()
def main():
    client =None
    #client = Neo4jClient()
    #client.delete_all()

    processor = BgeeProcessor()
    dump_processor(client, processor)


if __name__ == '__main__':
    main()
