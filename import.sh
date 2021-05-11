#!/usr/bin/env bash
NEO4J_VERSION=$(neo4j version | cut -f 2 -d ' ')
NEO4J_CONFIG=$(neo4j console | grep "^\s*config" | sed 's/^[ \t]*data:[ \t]*//g')
NEO4J_DATA=$(neo4j console | grep "^\s*data" | sed 's/^[ \t]*data:[ \t]*//g')

# Output commands as you go
set -x

neo4j stop
rm import.report

# Delete the old database and associated transactions
rm -rf $NEO4J_DATA/databases/indra
rm -rf $NEO4J_DATA/transactions/indra

# Just show what it is. This should match the --database option used below
cat $NEO4J_CONFIG/neo4j.conf | grep "dbms\.default_database"

python -m indra_cogex.sources --load

neo4j start
