neo4j stop
rm import.report
rm -rf /usr/local/var/neo4j/data/databases/indra
rm -rf /usr/local/var/neo4j/data/transactions/indra
cat /usr/local/Cellar/neo4j/4.1.3/libexec/conf/neo4j.conf | grep "dbms\.default_database"
neo4j-admin import \
  --database=indra \
  --delimiter='TAB' \
  --skip-duplicate-nodes=true \
  --skip-bad-relationships=true \
  --nodes=/Users/cthoyt/.data/indra/cogex/bgee/nodes/BioEntity.tsv.gz \
  --relationships=/Users/cthoyt/.data/indra/cogex/bgee/edges/BioEntity_expressed_in_BioEntity.tsv.gz \
  --nodes=/Users/cthoyt/.data/indra/cogex/ontology/nodes/BioEntity.tsv.gz \
  --relationships=/Users/cthoyt/.data/indra/cogex/ontology/edges/BioEntity_isa_BioEntity.tsv.gz \
  --relationships=/Users/cthoyt/.data/indra/cogex/ontology/edges/BioEntity_partof_BioEntity.tsv.gz \
  --relationships=/Users/cthoyt/.data/indra/cogex/ontology/edges/BioEntity_xref_BioEntity.tsv.gz \
  --nodes=/Users/cthoyt/.data/indra/cogex/database/nodes/BioEntity.tsv.gz \
  --relationships=/Users/cthoyt/.data/indra/cogex/database/edges/BioEntity_Complex_BioEntity.tsv.gz \
  --relationships=/Users/cthoyt/.data/indra/cogex/database/edges/BioEntity_Activation_BioEntity.tsv.gz \
  --relationships=/Users/cthoyt/.data/indra/cogex/database/edges/BioEntity_Inhibition_BioEntity.tsv.gz \
  --relationships=/Users/cthoyt/.data/indra/cogex/database/edges/BioEntity_IncreaseAmount_BioEntity.tsv.gz \
  --relationships=/Users/cthoyt/.data/indra/cogex/database/edges/BioEntity_DecreaseAmount_BioEntity.tsv.gz
neo4j start
