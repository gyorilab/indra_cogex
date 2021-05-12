__all__ = ["Neo4jClient"]

import logging
from typing import Any, List, Mapping, Optional, Tuple, Union

import neo4j
import neo4j.graph
from neo4j import GraphDatabase

from indra.statements import Agent
from indra.ontology.standardize import get_standard_agent
from indra_cogex.representation import Node, Relation

logger = logging.getLogger(__name__)

NEO4J_URL = "bolt://localhost:7687"


class Neo4jClient:
    """A client to communicate with an INDRA CogEx neo4j instance

    Parameters
    ----------
    url :
        The bolt URL to the neo4j instance.
    auth :
        A tuple consisting of the user name and password for the neo4j instance.
    """

    def __init__(
        self,
        url: Optional[str] = NEO4J_URL,
        auth: Optional[Tuple[str, str]] = None,
    ):
        self.url = url
        self.driver = GraphDatabase.driver(self.url, auth=auth)
        self.session = None

    def create_tx(
        self,
        query: str,
        query_params: Optional[Mapping[str, Any]] = None,
    ):
        """Run a transaction which writes to the neo4j instance.

        Parameters
        ----------
        query :
            The query string to be executed.
        query_params :
            Parameters associated with the query.
        """
        tx = self.get_session().begin_transaction()
        try:
            # logger.info(query)
            tx.run(query, parameters=query_params)
            tx.commit()
        except Exception as e:
            logger.error(e)
        finally:
            tx.close()

    def query_tx(self, query: str) -> Union[List[List[Any]], None]:
        """Run a read-only query and return the results.

        Parameters
        ----------
        query :
            The query string to be executed.

        Returns
        -------
        values :
            A list of results where each result is a list of one or more
            objects (typically neo4j nodes or relations).
        """
        tx = self.get_session().begin_transaction()
        try:
            res = tx.run(query)
        except Exception as e:
            logger.error(e)
            tx.close()
            return
        values = res.values()
        tx.close()
        return values

    def get_session(self, renew: Optional[bool] = False) -> neo4j.work.simple.Session:
        """Return an existing session or create one if needed.

        Parameters
        ----------
        renew :
            If True, a new session is created. Default: False

        Returns
        -------
        session
            A neo4j session.
        """
        if self.session is None or renew:
            sess = self.driver.session()
            self.session = sess
        return self.session

    def has_relation(self, source: str, target: str, relation: str) -> bool:
        """Return True if there is a relation between the source and the target.

        Parameters
        ----------
        source :
             Source identifier.
        target :
            Target identifier.
        relation :
            Relation type.

        Returns
        -------
        related :
            True if there is a relation of the given type, otherwise False.
        """
        res = self.get_relations(source, target, relation, limit=1)
        if res:
            return True
        else:
            return False

    def get_relations(
        self,
        source: Optional[str] = None,
        target: Optional[str] = None,
        relation: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[neo4j.graph.Relationship]:
        if not source and not target:
            raise ValueError("source or target should be specified")
        query = """
            MATCH (%s)-[r%s]->(%s)
            RETURN DISTINCT r
            %s
        """ % (
            "{id: '%s'}" % source if source else "s",
            "" if not relation else ":%s" % relation,
            "{id: '%s'}" % target if target else "t",
            "" if not limit else "LIMIT %s" % limit,
        )
        rels = [res[0] for res in self.query_tx(query)]
        return rels

    def get_source_relations(
        self,
        target: str,
        relation: Optional[str] = None,
    ) -> List[neo4j.graph.Relationship]:
        return self.get_relations(source=None, target=target, relation=relation)

    def get_target_relations(
        self,
        source: str,
        relation: Optional[str] = None,
    ) -> List[neo4j.graph.Relationship]:
        return self.get_relations(source=source, target=None, relation=relation)

    def get_targets(self, source, relation=None):
        return self.get_common_targets([source], relation)

    def get_sources(self, target, relation=None):
        return self.get_common_sources([target], relation)

    def get_common_sources(self, targets, relation):
        parts = ["(s)-[:%s]->({id: '%s'})" % (relation, target) for target in targets]
        query = """
            MATCH %s
            RETURN DISTINCT s
        """ % ",".join(
            parts
        )
        return self.query_tx(query)

    def get_common_targets(
        self,
        sources: List[str],
        relation: str,
    ) -> List[neo4j.graph.Node]:
        parts = ["({id: '%s'})-[:%s]->(t)" % (source, relation) for source in sources]
        query = """
            MATCH %s
            RETURN DISTINCT t
        """ % ",".join(
            parts
        )
        nodes = [res[0] for res in self.query_tx(query)]
        return nodes

    def get_target_agents(self, source: str, relation: str) -> List[Agent]:
        targets = self.get_targets(source, relation)
        agents = [self.node_to_agent(target) for target in targets]
        return agents

    def get_source_agents(self, target: str, relation: str) -> List[Agent]:
        sources = self.get_sources(target, relation)
        agents = [self.node_to_agent(source) for source in sources]
        return agents

    @staticmethod
    def node_to_agent(node: neo4j.graph.Node) -> Agent:
        name = node.get("name")
        grounding = node.get("id")
        if not name:
            name = grounding
        db_ns, db_id = grounding.split(":", maxsplit=1)
        return get_standard_agent(name, {db_ns: db_id})

    def delete_all(self):
        query = """MATCH(n) DETACH DELETE n"""
        return self.create_tx(query)

    def create_nodes(self, nodes: List[Node]):
        nodes_str = ",\n".join([str(n) for n in nodes])
        query = """CREATE %s""" % nodes_str
        return self.create_tx(query)

    def add_nodes(self, nodes: List[Node]):
        if not nodes:
            return
        prop_str = ",\n".join(["n.%s = node.%s" % (k, k) for k in nodes[0].data])
        # labels_str = ':'.join(nodes[0].labels)
        query = (
            """
            UNWIND $nodes AS node
            MERGE (n {id: node.id})
            SET %s
            WITH n, node
            CALL apoc.create.addLabels(n, node.labels)
            YIELD n
        """
            % prop_str
        )
        return self.create_tx(
            query,
            query_params={
                "nodes": [dict(**n.to_json()["data"], labels=n.labels) for n in nodes]
            },
        )

    def add_relations(self, relations: List[Relation]):
        if not relations:
            return None
        labels_str = ":".join(relations[0].labels)
        prop_str = ",\n".join(
            ["rel.%s = relation.%s" % (k, k) for k in relations[0].data]
        )
        query = """
            UNWIND $relations AS relation
            MATCH (e1 {id: relation.source_id}), (e2 {id: relation.target_id})
            MERGE (e1)-[rel:%s]->(e2)
            SET %s
        """ % (
            labels_str,
            prop_str,
        )
        rel_params = []
        for rel in relations:
            rd = dict(source_id=rel.source_id, target_id=rel.target_id, **rel.data)
            rel_params.append(rd)
        return self.create_tx(query, query_params={"relations": rel_params})

    def add_node(self, node: Node):
        prop_str = ",\n".join(["n.%s = '%s'" % (k, v) for k, v in node.data.items()])
        query = """
            MERGE (n:%s {id: '%s'})
            SET %s
        """ % (
            node.labels,
            node.identifier,
            prop_str,
        )
        return self.create_tx(query)

    def add_relation(self, relation: Relation):
        pass
