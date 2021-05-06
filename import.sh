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
  --nodes=$COGEX/bgee/nodes/BioEntity.tsv.gz \
  --relationships=$COGEX/bgee/edges/BioEntity_expressed_in_BioEntity.tsv.gz \
  --nodes=$COGEX/ontology/nodes/BioEntity.tsv.gz \
  --relationships=$COGEX/ontology/edges/BioEntity_isa_BioEntity.tsv.gz \
  --relationships=$COGEX/ontology/edges/BioEntity_partof_BioEntity.tsv.gz \
  --relationships=$COGEX/ontology/edges/BioEntity_xref_BioEntity.tsv.gz \
  --nodes=$COGEX/database/nodes/BioEntity.tsv.gz \
  --relationships=$COGEX/database/edges/BioEntity_Complex_BioEntity.tsv.gz \
  --relationships=$COGEX/database/edges/BioEntity_Activation_BioEntity.tsv.gz \
  --relationships=$COGEX/database/edges/BioEntity_Inhibition_BioEntity.tsv.gz \
  --relationships=$COGEX/database/edges/BioEntity_IncreaseAmount_BioEntity.tsv.gz \
  --relationships=$COGEX/database/edges/BioEntity_DecreaseAmount_BioEntity.tsv.gz

neo4j start
