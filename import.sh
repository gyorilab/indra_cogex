#!/usr/bin/env bash

# Output commands as you go
set -x

neo4j stop
rm import.report

# Delete the old database and associated transactions
rm -rf /usr/local/var/neo4j/data/databases/indra
rm -rf /usr/local/var/neo4j/data/transactions/indra

# Just show what it is. This should match the --database option used below
cat /usr/local/Cellar/neo4j/4.1.3/libexec/conf/neo4j.conf | grep "dbms\.default_database"

COGEX=$(pystow join indra cogex)
echo "INDRA COGEX directory: $COGEX"

neo4j-admin import \
  --database=indra \
  --delimiter='TAB' \
  --skip-duplicate-nodes=true \
  --skip-bad-relationships=true \
  --nodes=$COGEX/bgee/nodes.tsv.gz \
  --relationships=$COGEX/bgee/edges.tsv.gz \
  --nodes=$COGEX/ontology/nodes.tsv.gz \
  --relationships=$COGEX/ontology/edges.tsv.gz \
  --nodes=$COGEX/database/nodes.tsv.gz \
  --relationships=$COGEX/database/edges.tsv.gz \
  --nodes=$COGEX/reactome/nodes.tsv.gz \
  --relationships=$COGEX/reactome/edges.tsv.gz

neo4j start
