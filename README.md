INDRA CoGEx
===========
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
