#!/usr/bin/env bash
SUDO=false  # Set to true to run as sudo


if [[ $SUDO == "true" ]]
then
   NEO4J_PREFIX="sudo"
   COGEX_SUDO_ARG="--with-sudo"
else
   NEO4J_PREFIX=""
   COGEX_SUDO_ARG=""
fi

echo "NEO4J_PREFIX=$NEO4J_PREFIX"
echo "COGEX_SUDO_ARG=$COGEX_SUDO_ARG"


# NEO4J_VERSION=$(neo4j version | cut -f 2 -d ' ')
NEO4J_CONFIG=$($NEO4J_PREFIX neo4j console | grep "^\s*config" | sed 's/^[ \t]*config:[ \t]*//g')
NEO4J_DATA=$($NEO4J_PREFIX neo4j console | grep "^\s*data" | sed 's/^[ \t]*data:[ \t]*//g')

echo "NEO4J_CONFIG=$NEO4J_CONFIG"
echo "NEO4J_DATA=$NEO4J_DATA"

# Output commands as you go
set -x

$NEO4J_PREFIX neo4j stop
$NEO4J_PREFIX rm import.report

# Delete the old database and associated transactions
$NEO4J_PREFIX rm -rf $NEO4J_DATA/databases/indra
$NEO4J_PREFIX rm -rf $NEO4J_DATA/transactions/indra

# Just show what it is. This should match the --database option used below
cat $NEO4J_CONFIG/neo4j.conf | grep "dbms\.default_database"


# The import can take significant time (4+ hours) and memory.
python -m indra_cogex.sources --process --assemble --run-import $COGEX_SUDO_ARG

$NEO4J_PREFIX neo4j start

# Get the port
NEO4J_PORT=$(cat $NEO4J_CONFIG/neo4j.conf | grep "http.listen_address" | tr -d -c 0-9)

# Wait for the server to start up
echo "Waiting for database"
until [ \
  "$(curl -s -w '%{http_code}' -o /dev/null "http://localhost:$NEO4J_PORT")" \
  -eq 200 ]
do
  sleep 5
done

# Build the indexes
python -m indra_cogex.indexing --all --exist-ok
