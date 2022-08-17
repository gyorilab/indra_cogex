#!/usr/bin/env bash
SUDO=false  # Set to true to run as sudo


if [[ $SUDO == "true" ]]
then
   NEO4J_PREFIX="sudo"
   COGEX_SUDO_ARG="--with_sudo"
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


python -m indra_cogex.sources --process --assemble --run_import $COGEX_SUDO_ARG

$NEO4J_PREFIX neo4j start

# Wait for the server to start up and then build the indexes
sleep 10
python -m indra_cogex.indexing --all --exist-ok
