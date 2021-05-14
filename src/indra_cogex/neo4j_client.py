__all__ = ["Neo4jClient"]

import logging
from typing import Any, List, Mapping, Optional, Set, Tuple, Union

import neo4j
import neo4j.graph
from neo4j import GraphDatabase

from indra.databases import identifiers
from indra.statements import Agent
from indra.ontology.standardize import get_standard_agent
from indra_cogex.representation import Node, Relation
from indra_cogex.sources.processor import norm_id

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

    def has_relation(
        self, source: Tuple[str, str], target: Tuple[str, str], relation: str
    ) -> bool:
        """Return True if there is a relation between the source and the target.

        Parameters
        ----------
        source :
             Source namespace and identifier.
        target :
            Target namespace and identifier.
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
        source: Optional[Tuple[str, str]] = None,
        target: Optional[Tuple[str, str]] = None,
        relation: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Relation]:
        if not source and not target:
            raise ValueError("source or target should be specified")
        source = norm_id(*source) if source else None
        target = norm_id(*target) if target else None
        query = """
            MATCH p=(%s)-[%s]->(%s)
            RETURN DISTINCT p
            %s
        """ % (
            "{id: '%s'}" % source if source else "s",
            "" if not relation else ":%s" % relation,
            "{id: '%s'}" % target if target else "t",
            "" if not limit else "LIMIT %s" % limit,
        )
        rels = [self.neo4j_to_relation(res[0]) for res in self.query_tx(query)]
        return rels

    def get_source_relations(
        self,
        target: Tuple[str, str],
        relation: Optional[str] = None,
    ) -> List[Relation]:
        return self.get_relations(source=None, target=target, relation=relation)

    def get_target_relations(
        self,
        source: Tuple[str, str],
        relation: Optional[str] = None,
    ) -> List[Relation]:
        return self.get_relations(source=source, target=None, relation=relation)

    def get_all_relations(
        self,
        node: Tuple[str, str],
        relation: Optional[str] = None,
    ) -> List[Relation]:
        source_rels = self.get_source_relations(target=node, relation=relation)
        target_rels = self.get_target_relations(source=node, relation=relation)
        all_rels = source_rels + target_rels
        return all_rels

    def get_property_from_relations(
        self, relations: List[Relation], prop: str
    ) -> Set[str]:
        """Return the set of property values on given relations.

        Parameters
        ----------
        relations :
            The relations, each of which may or may not contain a value for
            the given property.
        prop :
            The key/name of the property to look for on each relation.

        Returns
        -------
        props
            A set of the values of the given property on the given list
            of relations.
        """
        props = {rel.data[prop] for rel in relations if prop in rel.data}
        return props

    def get_sources(self, target: Tuple[str, str], relation: str = None) -> List[Node]:
        """Return the nodes related to the target via a given relation type.

        Parameters
        ----------
        target :
            The target node's ID.
        relation :
            The relation label to constrain to when finding sources.

        Returns
        -------
        sources
            A list of source nodes.
        """
        return self.get_common_sources([target], relation)

    def get_common_sources(
        self, targets: List[Tuple[str, str]], relation: str
    ) -> List[Node]:
        """Return the common source nodes related to all the given targets
        via a given relation type.

        Parameters
        ----------
        targets :
            The target nodes' IDs.
        relation :
            The relation label to constrain to when finding sources.

        Returns
        -------
        sources
            A list of source nodes.
        """
        rel_str = ":%s" % relation if relation else ""
        parts = [
            "(s)-[%s]->({id: '%s'})" % (rel_str, norm_id(*target)) for target in targets
        ]
        query = """
            MATCH %s
            RETURN DISTINCT s
        """ % ",".join(
            parts
        )
        nodes = [self.neo4j_to_node(res[0]) for res in self.query_tx(query)]
        return nodes

    def get_targets(
        self, source: Tuple[str, str], relation: Optional[str] = None
    ) -> List[Node]:
        """Return the nodes related to the source via a given relation type.

        Parameters
        ----------
        source :
            The source node's ID.
        relation :
            The relation label to constrain to when finding targets.

        Returns
        -------
        targets
            A list of target nodes.
        """
        return self.get_common_targets([source], relation)

    def get_common_targets(
        self,
        sources: List[str],
        relation: str,
    ) -> List[Node]:
        """Return the common target nodes related to all the given sources
        via a given relation type.

        Parameters
        ----------
        sources :
            The source nodes' IDs.
        relation :
            The relation label to constrain to when finding targets.

        Returns
        -------
        targets
            A list of target nodes.
        """
        rel_str = ":%s" % relation if relation else ""
        parts = [
            "({id: '%s'})-[%s]->(t)" % (norm_id(*source), rel_str) for source in sources
        ]
        query = """
            MATCH %s
            RETURN DISTINCT t
        """ % ",".join(
            parts
        )
        nodes = [self.neo4j_to_node(res[0]) for res in self.query_tx(query)]
        return nodes

    def get_target_agents(self, source: Tuple[str, str], relation: str) -> List[Agent]:
        """Return the nodes related to the source via a given relation type as INDRA Agents.

        Parameters
        ----------
        source :
            The source node's ID.
        relation :
            The relation label to constrain to when finding targets.

        Returns
        -------
        targets
            A list of target nodes as INDRA Agents.
        """
        targets = self.get_targets(source, relation)
        agents = [self.node_to_agent(target) for target in targets]
        return agents

    def get_source_agents(self, target: Tuple[str, str], relation: str) -> List[Agent]:
        """Return the nodes related to the target via a given relation type as INDRA Agents.

        Parameters
        ----------
        target :
            The target node's ID.
        relation :
            The relation label to constrain to when finding sources.

        Returns
        -------
        sources
            A list of source nodes as INDRA Agents.
        """
        sources = self.get_sources(target, relation)
        agents = [self.node_to_agent(source) for source in sources]
        return agents

    @staticmethod
    def neo4j_to_node(neo4j_node: neo4j.graph.Node) -> Node:
        props = dict(neo4j_node)
        node_id = props.pop("id")
        db_ns, db_id = process_identifier(node_id)
        return Node(db_ns, db_id, neo4j_node.labels, props)

    @staticmethod
    def neo4j_to_relation(neo4j_path: neo4j.graph.Path) -> Relation:
        neo4j_relation = neo4j_path.relationships[0]
        rel_type = neo4j_relation.type
        props = dict(neo4j_relation)
        source_ns, source_id = process_identifier(neo4j_relation.start_node)
        target_ns, target_id = process_identifier(neo4j_relation.end_node)
        return Relation(source_ns, source_id, target_ns, target_id, rel_type, props)

    @staticmethod
    def node_to_agent(node: Node) -> Agent:
        """Return an INDRA Agent from a graph node."""
        name = node.data.get("name")
        if not name:
            name = f"{node.db_ns}:{node.db_id}"
        return get_standard_agent(name, {node.db_ns: node.db_id})

    def delete_all(self):
        """Delete everything in the neo4j database."""
        query = """MATCH(n) DETACH DELETE n"""
        return self.create_tx(query)

    def create_nodes(self, nodes: List[Node]):
        """Create a set of new graph nodes."""
        nodes_str = ",\n".join([str(n) for n in nodes])
        query = """CREATE %s""" % nodes_str
        return self.create_tx(query)

    def add_nodes(self, nodes: List[Node]):
        """Merge a set of graph nodes (create or update)."""
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
        """Merge a set of graph relations (create or update)."""
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
        """Merge a single node into the graph."""
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


def process_identifier(identifier):
    graph_ns, graph_id = identifier.split(":", maxsplit=1)
    db_ns, db_id = identifiers.get_ns_id_from_identifiers(graph_ns, graph_id)
    db_id = identifiers.ensure_prefix_if_needed(db_ns, db_id)
    return db_ns, db_id
