#!/usr/bin/env bash
NEO4J_VERSION=4.2.0
NEO4J_PATH=/usr/local/Cellar/neo4j/$NEO4J_VERSION
NEO4J_DATA=/usr/local/var/neo4j/data

# Output commands as you go
set -x

neo4j stop
rm import.report

# Delete the old database and associated transactions
rm -rf $NEO4J_DATA/databases/indra
rm -rf $NEO4J_DATA/transactions/indra

# Just show what it is. This should match the --database option used below
cat $NEO4J_PATH/libexec/conf/neo4j.conf | grep "dbms\.default_database"

python -m indra_cogex.sources --load

neo4j start
