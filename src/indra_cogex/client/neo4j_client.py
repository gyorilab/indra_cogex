"""Neo4j client module."""

import inspect
import logging
from functools import lru_cache, wraps
from typing import Any, Iterable, List, Mapping, Optional, Set, Tuple, Union

import neo4j.graph
from indra.config import get_config
from indra.databases import identifiers
from indra.ontology.standardize import get_standard_agent
from indra.statements import Agent
from neo4j import GraphDatabase

from indra_cogex.representation import Node, Relation, norm_id, triple_query

__all__ = ["Neo4jClient", "autoclient"]

logger = logging.getLogger(__name__)


class Neo4jClient:
    """A client to communicate with an INDRA CogEx neo4j instance

    Parameters
    ----------
    url :
        The bolt URL to the neo4j instance to override INDRA_NEO4J_URL
        set as an environment variable or set in the INDRA config file.
    auth :
        A tuple consisting of the user name and password for the neo4j instance to
        override INDRA_NEO4J_USER and
        INDRA_NEO4J_PASSWORD set as environment variables or set in the INDRA config file.
    """

    #: The session
    session: Optional[neo4j.Session]

    def __init__(
        self,
        url: Optional[str] = None,
        auth: Optional[Tuple[str, str]] = None,
    ):
        """Initialize the Neo4j client."""
        self.driver = None
        self.session = None
        if not url:
            INDRA_NEO4J_URL = get_config("INDRA_NEO4J_URL")
            if INDRA_NEO4J_URL:
                url = INDRA_NEO4J_URL
                logger.info("Using configured URL for INDRA neo4j connection")
            else:
                logger.info("INDRA_NEO4J_URL not configured")
        if not auth:
            INDRA_NEO4J_USER = get_config("INDRA_NEO4J_USER")
            INDRA_NEO4J_PASSWORD = get_config("INDRA_NEO4J_PASSWORD")
            if INDRA_NEO4J_USER and INDRA_NEO4J_PASSWORD:
                auth = (INDRA_NEO4J_USER, INDRA_NEO4J_PASSWORD)
                logger.info("Using configured credentials for INDRA neo4j connection")
            else:
                logger.info("INDRA_NEO4J_USER and INDRA_NEO4J_PASSWORD not configured")
        # Set max_connection_lifetime to something smaller than the timeouts
        # on the server or on the way to the server. See
        # https://github.com/neo4j/neo4j-python-driver/issues/316#issuecomment-564020680
        self.driver = GraphDatabase.driver(
            url,
            auth=auth,
            max_connection_lifetime=3 * 60,
        )

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

    def get_session(self, renew: Optional[bool] = False) -> neo4j.Session:
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

    def close_session(self):
        """Close the session if it exists."""
        if self.session is not None:
            self.session.close()

    def has_relation(
        self,
        source: Tuple[str, str],
        target: Tuple[str, str],
        relation: str,
        source_type: Optional[str] = None,
        target_type: Optional[str] = None,
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
        source_type :
            A constraint on the source type
        target_type :
            A constraint on the target type

        Returns
        -------
        related :
            True if there is a relation of the given type, otherwise False.
        """
        res = self.get_relations(
            source,
            target,
            relation,
            limit=1,
            source_type=source_type,
            target_type=target_type,
        )
        if res:
            return True
        else:
            return False

    def get_relations(
        self,
        source: Optional[Tuple[str, str]] = None,
        target: Optional[Tuple[str, str]] = None,
        relation: Optional[str] = None,
        source_type: Optional[str] = None,
        target_type: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Relation]:
        """Return relations based on source, target and type constraints.

        This is a generic function for getting relations, all of its parameters
        are optional, though at least a source or a target needs to be provided.

        Parameters
        ----------
        source :
            Surce namespace and ID.
        target :
            Target namespace and ID.
        relation :
            Relation type.
        source_type :
            A constraint on the source type
        target_type :
            A constraint on the target type
        limit :
            A limit on the number of relations returned.

        Returns
        -------
        rels :
            A list of relations matching the constraints.
        """
        if not source and not target:
            raise ValueError("source or target should be specified")
        source = norm_id(*source) if source else None
        target = norm_id(*target) if target else None
        match = triple_query(
            source_id=source,
            source_type=source_type,
            relation_type=relation,
            target_id=target,
            target_type=target_type,
        )
        query = """
            MATCH p=%s
            RETURN DISTINCT p
            %s
        """ % (
            match,
            "" if not limit else "LIMIT %s" % limit,
        )
        rels = [self.neo4j_to_relation(res[0]) for res in self.query_tx(query)]
        return rels

    def get_source_relations(
        self,
        target: Tuple[str, str],
        relation: Optional[str] = None,
        target_type: Optional[str] = None,
    ) -> List[Relation]:
        """Get relations that connect sources to the given target.

        Parameters
        ----------
        target :
            Target namespace and identifier.
        relation :
            Relation type.

        Returns
        -------
        rels :
            A list of relations matching the constraints.
        """
        return self.get_relations(
            source=None, target=target, relation=relation, target_type=target_type
        )

    def get_target_relations(
        self,
        source: Tuple[str, str],
        relation: Optional[str] = None,
        source_type: Optional[str] = None,
    ) -> List[Relation]:
        """Get relations that connect targets from the given source.

        Parameters
        ----------
        source :
            Source namespace and identifier.
        relation :
            Relation type.

        Returns
        -------
        rels :
            A list of relations matching the constraints.
        """
        return self.get_relations(
            source=source, target=None, relation=relation, source_type=source_type
        )

    def get_all_relations(
        self,
        node: Tuple[str, str],
        relation: Optional[str] = None,
        source_type: Optional[str] = None,
        target_type: Optional[str] = None,
    ) -> List[Relation]:
        """Get relations that connect sources and targets with the given node.

        Parameters
        ----------
        node :
            Node namespace and identifier.
        relation :
            Relation type.
        source_type :
            Type constraint on the sources for in-edges
        target_type :
            Type constraint on te targets for out-edges

        Returns
        -------
        rels :
            A list of relations matching the constraints.
        """
        source_rels = self.get_source_relations(
            target=node, relation=relation, source_type=source_type
        )
        target_rels = self.get_target_relations(
            source=node, relation=relation, target_type=target_type
        )
        all_rels = source_rels + target_rels
        return all_rels

    @staticmethod
    def get_property_from_relations(relations: List[Relation], prop: str) -> Set[str]:
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

    def get_sources(
        self,
        target: Tuple[str, str],
        relation: str = None,
        source_type: Optional[str] = None,
        target_type: Optional[str] = None,
    ) -> List[Node]:
        """Return the nodes related to the target via a given relation type.

        Parameters
        ----------
        target :
            The target node's ID.
        relation :
            The relation label to constrain to when finding sources.
        source_type :
            A constraint on the source type
        target_type :
            A constraint on the target type

        Returns
        -------
        sources
            A list of source nodes.
        """
        return self.get_common_sources(
            [target],
            relation,
            source_type=source_type,
            target_type=target_type,
        )

    def get_common_sources(
        self,
        targets: List[Tuple[str, str]],
        relation: str,
        source_type: Optional[str] = None,
        target_type: Optional[str] = None,
    ) -> List[Node]:
        """Return the common source nodes related to all the given targets
        via a given relation type.

        Parameters
        ----------
        targets :
            The target nodes' IDs.
        relation :
            The relation label to constrain to when finding sources.
        source_type :
            A constraint on the source type
        target_type :
            A constraint on the target type

        Returns
        -------
        sources
            A list of source nodes.
        """
        parts = [
            triple_query(
                source_name="s",
                source_type=source_type,
                relation_type=relation,
                target_id=norm_id(*target),
                target_type=target_type,
            )
            for target in targets
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
        self,
        source: Tuple[str, str],
        relation: Optional[str] = None,
        source_type: Optional[str] = None,
        target_type: Optional[str] = None,
    ) -> List[Node]:
        """Return the nodes related to the source via a given relation type.

        Parameters
        ----------
        source :
            Source namespace and identifier.
        relation :
            The relation label to constrain to when finding targets.
        source_type :
            A constraint on the source type
        target_type :
            A constraint on the target type

        Returns
        -------
        targets
            A list of target nodes.
        """
        return self.get_common_targets(
            [source],
            relation,
            source_type=source_type,
            target_type=target_type,
        )

    def get_common_targets(
        self,
        sources: List[Tuple[str, str]],
        relation: str,
        source_type: Optional[str] = None,
        target_type: Optional[str] = None,
    ) -> List[Node]:
        """Return the common target nodes related to all the given sources
        via a given relation type.

        Parameters
        ----------
        sources :
            Source namespace and identifier.
        relation :
            The relation label to constrain to when finding targets.
        source_type :
            A constraint on the source type
        target_type :
            A constraint on the target type

        Returns
        -------
        targets
            A list of target nodes.
        """
        parts = [
            triple_query(
                source_id=norm_id(*source),
                source_type=source_type,
                relation_type=relation,
                target_name="t",
                target_type=target_type,
            )
            for source in sources
        ]
        query = """
            MATCH %s
            RETURN DISTINCT t
        """ % ",".join(
            parts
        )
        nodes = [self.neo4j_to_node(res[0]) for res in self.query_tx(query)]
        return nodes

    def get_target_agents(
        self,
        source: Tuple[str, str],
        relation: str,
        source_type: Optional[str] = None,
    ) -> List[Agent]:
        """Return the nodes related to the source via a given relation type as INDRA Agents.

        Parameters
        ----------
        source :
            Source namespace and identifier.
        relation :
            The relation label to constrain to when finding targets.
        source_type :
            A constraint on the source type

        Returns
        -------
        targets
            A list of target nodes as INDRA Agents.
        """
        targets = self.get_targets(source, relation, source_type=source_type)
        agents = [self.node_to_agent(target) for target in targets]
        return agents

    def get_source_agents(self, target: Tuple[str, str], relation: str) -> List[Agent]:
        """Return the nodes related to the target via a given relation type as INDRA Agents.

        Parameters
        ----------
        target :
            Target namespace and identifier.
        relation :
            The relation label to constrain to when finding sources.

        Returns
        -------
        sources
            A list of source nodes as INDRA Agents.
        """
        sources = self.get_sources(
            target,
            relation,
            source_type="BioEntity",
            target_type="BioEntity",
        )
        agents = [self.node_to_agent(source) for source in sources]
        return agents

    def get_predecessors(
        self,
        target: Tuple[str, str],
        relations: Iterable[str],
        source_type: Optional[str] = None,
        target_type: Optional[str] = None,
    ) -> List[Node]:
        """Return the nodes that precede the given node via the given relation types.

        Parameters
        ----------
        target :
            The target node's ID.
        relations :
            The relation labels to constrain to when finding predecessors.
        source_type :
            A constraint on the source type
        target_type :
            A constraint on the target type

        Returns
        -------
        predecessors
            A list of predecessor nodes.
        """
        match = triple_query(
            source_name="s",
            source_type=source_type,
            relation_type="%s*1.." % "|".join(relations),
            target_id=norm_id(*target),
            target_type=target_type,
        )
        query = (
            """
            MATCH %s
            RETURN DISTINCT s
        """
            % match
        )
        nodes = [self.neo4j_to_node(res[0]) for res in self.query_tx(query)]
        return nodes

    def get_successors(
        self,
        source: Tuple[str, str],
        relations: Iterable[str],
        source_type: Optional[str] = None,
        target_type: Optional[str] = None,
    ) -> List[Node]:
        """Return the nodes that precede the given node via the given relation types.

        Parameters
        ----------
        source :
            The source node's ID.
        relations :
            The relation labels to constrain to when finding successors.
        source_type :
            A constraint on the source type
        target_type :
            A constraint on the target type

        Returns
        -------
        predecessors
            A list of predecessor nodes.
        """
        match = triple_query(
            source_id=norm_id(*source),
            source_type=source_type,
            relation_type="%s*1.." % "|".join(relations),
            target_name="t",
            target_type=target_type,
        )
        query = (
            """
            MATCH %s
            RETURN DISTINCT t
        """
            % match
        )
        nodes = [self.neo4j_to_node(res[0]) for res in self.query_tx(query)]
        return nodes

    @staticmethod
    def neo4j_to_node(neo4j_node: neo4j.graph.Node) -> Node:
        """Return a Node from a neo4j internal node.

        Parameters
        ----------
        neo4j_node :
            A neo4j internal node using its internal data structure and
            identifier scheme.

        Returns
        -------
        node :
            A Node object with the INDRA standard identifier scheme.
        """
        props = dict(neo4j_node)
        node_id = props.pop("id")
        db_ns, db_id = process_identifier(node_id)
        return Node(db_ns, db_id, neo4j_node.labels, props)

    @staticmethod
    def neo4j_to_relation(neo4j_path: neo4j.graph.Path) -> Relation:
        """Return a Relation from a neo4j internal single-relation path.

        Parameters
        ----------
        neo4j_path :
            A neo4j internal single-edge path using its internal data structure
            and identifier scheme.

        Returns
        -------
        relation :
            A Relation object with the INDRA standard identifier scheme.
        """
        return Neo4jClient.neo4j_to_relations(neo4j_path)[0]

    @staticmethod
    def neo4j_to_relations(neo4j_path: neo4j.graph.Path) -> List[Relation]:
        """Return a list of Relations from a neo4j internal multi-relation path.

        Parameters
        ----------
        neo4j_path :
            A neo4j internal single-edge path using its internal data structure
            and identifier scheme.

        Returns
        -------
        :
            A list of Relation objects with the INDRA standard identifier
            scheme.
        """
        relations = []
        for neo4j_relation in neo4j_path.relationships:
            rel_type = neo4j_relation.type
            props = dict(neo4j_relation)
            source_ns, source_id = process_identifier(neo4j_relation.start_node["id"])
            target_ns, target_id = process_identifier(neo4j_relation.end_node["id"])
            rel = Relation(source_ns, source_id, target_ns, target_id, rel_type, props)
            relations.append(rel)
        return relations

    @staticmethod
    def node_to_agent(node: Node) -> Agent:
        """Return an INDRA Agent from a Node.

        Parameters
        ----------
        node :
            A Node object.

        Returns
        -------
        agent :
            An INDRA Agent with standardized name and expanded/standardized
            db_refs.
        """
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
        labels_str = relations[0].rel_type
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
            norm_id(node.db_ns, node.db_id),
            prop_str,
        )
        return self.create_tx(query)


def process_identifier(identifier: str) -> Tuple[str, str]:
    """Process a neo4j-internal identifier string into an INDRA namespace and ID.

    Parameters
    ----------
    identifier :
        An identifier string (containing both prefix and ID) corresponding
        to an internal neo4j graph node.

    Returns
    -------
    db_ns:
        An INDRA-standard namespace corresponding to the input identifier.
    db_id:
        An INDRA-standard identifier corresponding to the input identifier.
    """
    graph_ns, graph_id = identifier.split(":", maxsplit=1)
    db_ns, db_id = identifiers.get_ns_id_from_identifiers(graph_ns, graph_id)
    # This is a corner case where the prefix is not in the registry
    # and in those cases we just use the upper case version of the prefix
    # in the graph to revert it to the INDRA-compatible key.
    if not db_ns:
        db_ns = graph_ns.upper()
        db_id = graph_id
    else:
        db_id = identifiers.ensure_prefix_if_needed(db_ns, db_id)
    return db_ns, db_id


def autoclient(*, cache: bool = False, maxsize: Optional[int] = 128):
    """Wrap a function that takes a client for easier usage.


    Arguments
    ---------
    cache :
        Should the result be cached using :func:`functools.lru_cache`? Is
        False by default.
    maxsize :
        If cache is True, this is the value passed to the ``maxsize`` argument
        of :func:`functools.lru_cache`. Set to None for unlimited caching, but
        beware that this can potentially use a lot of memory and isn't a good
        idea for queries that can take a lot of different kinds of input over
        time.

    Returns
    -------
        : A decorator object that will wrap the function

    Usage
    -----
    Not appropriate for caching (i.e., many possible inputs, especially
    in a web app scenario)::

    .. code-block:: python

        @autoclient()
        def get_tissues_for_gene(gene: Tuple[str, str], *, client: Neo4jClient):
            return client.get_targets(
                gene,
                relation="expressed_in",
                source_type="BioEntity",
                target_type="BioEntity",
            )

    Appropriate for caching (e.g., doen't take inputs at all)::

    .. code-block:: python

        @autoclient(cache=True, maxsize=1)
        def get_node_count(*, client: Neo4jClient) -> Counter:
            return Counter(
                {
                    label[0]: client.query_tx(f"MATCH (n:{label[0]}) RETURN count(*)")[0][0]
                    for label in client.query_tx("call db.labels();")
                }
            )
    """

    def _decorator(func):
        signature = inspect.signature(func)
        client_param = signature.parameters.get("client")
        if client_param is None:
            raise ValueError(
                "the autoclient decorator can't be applied to a function that"
                " doesn't take a neo4j client."
            )
        if client_param.kind != inspect.Parameter.KEYWORD_ONLY:
            raise ValueError(
                "the autoclient decorator can't be applied to a function whose"
                " `client` argument isn't keyword-only"
            )

        @wraps(func)
        def _wrapped(*args, **kwargs):
            client = kwargs.get("client")
            if client is None:
                kwargs["client"] = Neo4jClient()
            rv = func(*args, **kwargs)
            if client is None:
                kwargs["client"].close_session()
            return rv

        if cache:
            _wrapped = lru_cache(maxsize=maxsize)(_wrapped)

        return _wrapped

    return _decorator
