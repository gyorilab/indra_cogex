__all__ = ['Neo4jClient']

import logging
from typing import List
from neo4j import GraphDatabase

from indra.statements import *
from indra_cogex.representation import Node, Relation


logger = logging.getLogger(__name__)


NEO4J_URL = 'bolt://localhost:7687'


class Neo4jClient:
    def __init__(self, url=NEO4J_URL, auth=None):
        self.url = url
        self.driver = GraphDatabase.driver(self.url, auth=auth)
        self.session = None

    def create_tx(self, query, query_params=None):
        tx = self.get_session().begin_transaction()
        try:
            logger.info(query)
            tx.run(query, parameters=query_params)
            tx.commit()
        except Exception as e:
            logger.error(e)
        finally:
            tx.close()

    def query_tx(self, query):
        tx = self.get_session().begin_transaction()
        res = tx.run(query)
        values = res.values()
        tx.close()
        return values

    def get_session(self, renew=False):
        if self.session is None or renew:
            sess = self.driver.session()
            self.session = sess
        return self.session

    def has_relation(self, source, target, relation):
        query = """
            MATCH p=({id: '%s'})-[r:%s]->({id: '%s'})
            RETURN p
            LIMIT 1
        """ % (source, relation, target)
        res = self.query_tx(query)
        if res:
            return True
        else:
            return False

    def get_relations(self, source, target):
        query = """
            MATCH p=({id: '%s'})-[r]->({id: '%s'})
            RETURN r
            LIMIT 1
        """ % (source, target)
        res = self.query_tx(query)
        return res

    def get_targets(self, source, relation):
        query = """
            MATCH p=({id: '%s'})-[r:%s]->(t)
            RETURN t
        """ % (source, relation)
        return self.query_tx(query)

    def get_sources(self, relation, target):
        query = """
            MATCH p=(s)-[r:%s]->({id: '%s'})
            RETURN s
        """ % (relation, target)
        return self.query_tx(query)

    def get_target_agents(self, source, relation):
        paths = self.get_targets(source, relation)
        agents = [self.path_to_agents(path)[0] for path in paths]
        return agents

    def get_source_agents(self, relation, target):
        paths = self.get_sources(relation, target)
        agents = [self.path_to_agents(path)[0] for path in paths]
        return agents

    def path_to_agents(self, path):
        return [self.node_to_agent(node) for node in path]

    @staticmethod
    def node_to_agent(node):
        name = node.get('name')
        grounding = node.get('id')
        if not name:
            name = grounding
        db_ns, db_id = grounding.split(':', maxsplit=1)
        return Agent(name, db_refs={db_ns: db_id})

    def delete_all(self):
        query = """MATCH(n) DETACH DELETE n"""
        return self.create_tx(query)

    def add_nodes(self, nodes: List[Node]):
        if not nodes:
            return
        prop_str = ',\n'.join(['n.%s = node.%s' % (k, k)
                               for k in nodes[0].data])
        labels_str = ':'.join(nodes[0].labels)
        query = """
            UNWIND $nodes AS node
            MERGE (n:%s {id: node.id})
            SET %s
        """ % (labels_str, prop_str)
        return self.create_tx(
            query, query_params={'nodes': [n.to_json()['data'] for n in nodes]})

    def add_relations(self, relations: List[Relation]):
        if not relations:
            return None
        labels_str = ':'.join(relations[0].labels)
        prop_str = ',\n'.join(['rel.%s = relation.%s' % (k, k)
                               for k in relations[0].data])
        query = """
            UNWIND $relations AS relation
            MATCH (e1 {id: relation.source_id}), (e2 {id: relation.target_id})
            MERGE (e1)-[rel:%s]->(e2)
            SET %s
        """ % (labels_str, prop_str)
        rel_params = []
        for rel in relations:
            rd = dict(source_id=rel.source_id,
                      target_id=rel.target_id,
                      **rel.data)
            rel_params.append(rd)
        return self.create_tx(query, query_params={'relations': rel_params})

    def add_node(self, node: Node):
        prop_str = ',\n'.join(['n.%s = \'%s\'' % (k, v)
                               for k, v in node.data.items()])
        query = """
            MERGE (n:%s {id: '%s'})
            SET %s
        """ % (node.labels, node.identifier, prop_str)
        return self.create_tx(query)

    def add_relation(self, relation: Relation):
        pass
