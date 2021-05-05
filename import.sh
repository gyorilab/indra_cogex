neo4j stop
rm -rf /usr/local/var/neo4j/data/databases/indra
rm -rf /usr/local/var/neo4j/data/transactions/indra
cat /usr/local/Cellar/neo4j/4.1.3/libexec/conf/neo4j.conf | grep "dbms\.default_database"
neo4j-admin import \
  --database=indra \
  --skip-duplicate-nodes=true \
  --nodes=/Users/cthoyt/.data/indra/cogex/bgee/nodes/BioEntity.csv.gz \
  --relationships=/Users/cthoyt/.data/indra/cogex/bgee/edges/BioEntity_expressed_in_BioEntity.csv.gz \
  --nodes=/Users/cthoyt/.data/indra/cogex/ontology/nodes/BioEntity.csv.gz \
  --relationships=/Users/cthoyt/.data/indra/cogex/ontology/edges/BioEntity_isa_BioEntity.csv.gz \
  --relationships=/Users/cthoyt/.data/indra/cogex/ontology/edges/BioEntity_partof_BioEntity.csv.gz \
  --relationships=/Users/cthoyt/.data/indra/cogex/ontology/edges/BioEntity_xref_BioEntity.csv.gz \
#  --nodes=/Users/cthoyt/.data/indra/cogex/database/nodes/BioEntity.csv.gz \
  --relationships=/Users/cthoyt/.data/indra/cogex/database/edges/BioEntity_Complex_BioEntity.csv.gz \
  --relationships=/Users/cthoyt/.data/indra/cogex/database/edges/BioEntity_Activation_BioEntity.csv.gz \
  --relationships=/Users/cthoyt/.data/indra/cogex/database/edges/BioEntity_Inhibition_BioEntity.csv.gz \
  --relationships=/Users/cthoyt/.data/indra/cogex/database/edges/BioEntity_IncreaseAmount_BioEntity.csv.gz \
  --relationships=/Users/cthoyt/.data/indra/cogex/database/edges/BioEntity_DecreaseAmount_BioEntity.csv.gz
neo4j start
