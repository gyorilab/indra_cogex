#!/bin/bash

# Get neo4j data directory
NEO4J_CONFIG=/etc/neo4j/neo4j.conf
NEO4J_DATA=$(sudo grep -e '^server.directories.data' $NEO4J_CONFIG | sed 's/^server.directories.data=//g')

# Verify that we got a valid data directory
if [ -z "$NEO4J_DATA" ]; then
    echo "Could not determine Neo4j data directory from $NEO4J_CONFIG. Please check the
      configuration file."
    exit 1
fi
# Verify that the data directory exists
if [ ! -d "$NEO4J_DATA" ]; then
    echo "Neo4j data directory $NEO4J_DATA does not exist. Please check the configuration file."
    exit 1
fi

# Set the dump directory
DUMP_DIR="$(realpath "$(dirname "$NEO4J_DATA")/dumps")/$(date +%Y%m%d)"
mkdir -p "$DUMP_DIR"

# Make sure neo4j is stopped
sudo neo4j stop

# DUMP
sudo neo4j-admin database dump --to-path "$DUMP_DIR" neo4j

echo "Database dumped to directory $DUMP_DIR"

# Start neo4j again
sudo neo4j start
