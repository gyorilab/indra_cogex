INDRA CoGEx
===========
[![Tests](https://github.com/bgyori/indra_cogex/actions/workflows/tests.yml/badge.svg)](https://github.com/bgyori/indra_cogex/actions/workflows/tests.yml)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

INDRA CoGEx (Context Graph Extension) is an automatically assembled
biomedical knowledge graph which integrates causal mechanisms from INDRA with
non-causal contextual relations including properties, ontology, and data.


## Installation

Install the `indra_cogex` package with:

```shell
$ git clone https://github.com/bgyori/indra_cogex
$ cd indra_cogex
$ pip install -e .
```

## Build

Build the graph then bulk import into Neo4j with:

```shell
$ python -m indra_cogex.sources
$ sh import.sh
```

## Funding
The development of this project is funded under the DARPA Young Faculty Award
(ARO grant W911NF2010255).
