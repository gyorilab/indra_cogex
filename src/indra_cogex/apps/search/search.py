import json
from typing import List, Optional, Mapping, Tuple, Dict
import logging

from flask import Blueprint, render_template, request, jsonify, redirect, url_for, session
from flask_jwt_extended import jwt_required
from flask_wtf import FlaskForm
from indra.statements import get_all_descendants, Statement, Phosphorylation, IncreaseAmount, DecreaseAmount
from indra.statements.validate import assert_valid_id, BioregistryValidator
from wtforms import StringField, SubmitField
from wtforms.fields.simple import BooleanField
from wtforms.validators import DataRequired

from indra_cogex.apps.utils import render_statements
from indra_cogex.analysis.gene_analysis import parse_phosphosite_list
from indra_cogex.apps.utils import render_statements, resolve_email
from indra_cogex.client import Neo4jClient, autoclient
from indra_cogex.client.queries import *
from indra_cogex.representation import norm_id

logger = logging.getLogger(__name__)

__all__ = ["search_blueprint"]

from indra_cogex.client.queries import enrich_statements, check_agent_existence

from indra_cogex.representation import indra_stmts_from_relations

search_blueprint = Blueprint("search", __name__, url_prefix="/search")


class SearchForm(FlaskForm):
    agent_name = StringField("Agent Name", validators=[DataRequired()])
    agent_role = StringField("Agent Role")
    other_agent = StringField("Other Agent")
    other_agent_role = StringField("Other Agent Role")
    source_type = StringField("Source Type")
    rel_type = StringField("Relationship Type")
    left_arrow = BooleanField("⇐")
    right_arrow = BooleanField("➔")
    both_arrow = BooleanField("⇔")
    paper_id = StringField("Paper ID")
    mesh_terms = StringField("MeSH Terms")
    submit = SubmitField("Search")


@search_blueprint.route("/", methods=['GET', 'POST'])
@jwt_required(optional=True)
def search():
    stmt_types = {c.__name__ for c in get_all_descendants(Statement)}
    stmt_types -= {"Influence", "Event", "Unresolved"}
    stmt_types_json = json.dumps(sorted(list(stmt_types)))

    form = SearchForm()

    # POST Request: Generate a sharable link with query parameters
    if request.method == 'POST':
        agent = form.agent_name.data
        agent_tuple = json.loads(request.form.get("agent_tuple") or "null")

        if agent:
            if agent_tuple:
                agent, is_curie = check_and_convert(":".join(agent_tuple))
            else:
                is_curie = False
            agent_exists = check_agent_existence(agent)

            if not agent_exists:
                if is_curie:
                    error_agent = f'{agent[0]}:{agent[1]}'
                else:
                    error_agent = agent
                return render_template(
                    "search/search_page.html",
                    form=form,
                    stmt_types_json=stmt_types_json,
                    agent_not_found=True,
                    error_agent=error_agent
                )
        query_params = {
            "agent": form.agent_name.data,
            # JSON dumps for proper encoding
            "agent_tuple": json.dumps(agent_tuple) if agent_tuple else None,
            "other_agent": form.other_agent.data,
            "other_agent_tuple": request.form.get("other_agent_tuple"),
            "source_type": form.source_type.data,
            "rel_types": json.loads(form.rel_type.data) if form.rel_type.data else None,
            "agent_role": form.agent_role.data,
            "other_role": form.other_agent_role.data,
            "paper_id": form.paper_id.data,
            "mesh_terms": form.mesh_terms.data,
            "mesh_tuple": request.form.get("mesh_tuple"),
        }
        query_params = {k: v for k, v in query_params.items() if v}
        return redirect(url_for("search.search", **query_params))

    # GET Request: Extract query parameters and fetch statements
    agent = request.args.get("agent")

    # Check if the agent is in CURIE form
    if agent:
        agent, _ = check_and_convert(agent)

    agent_tuple = request.args.get("agent_tuple")
    if agent_tuple:
        source_db, source_id = json.loads(agent_tuple)
        agent = (source_db, source_id)

    other_agent = request.args.get("other_agent")

    # Check if the other_agent is in CURIE form
    if other_agent:
        other_agent, _ = check_and_convert(other_agent)

    other_agent_tuple = request.args.get("other_agent_tuple")
    if other_agent_tuple:
        source_db, source_id = json.loads(other_agent_tuple)
        other_agent = (source_db, source_id)

    source_type = request.args.get("source_type")
    rel_types = request.args.getlist("rel_types")

    agent_role = request.args.get("agent_role")
    other_role = request.args.get("other_role")
    paper_id = request.args.get("paper_id")
    mesh_terms = request.args.get("mesh_terms")

    # Check if the mesh_terms is in CURIE form
    if mesh_terms:
        mesh_terms, _ = check_and_convert(mesh_terms)

    mesh_tuple = request.args.get("mesh_tuple")
    if mesh_tuple:
        source_db, source_id = json.loads(mesh_tuple)
        mesh_terms = (source_db, source_id)
    # Fetch and display statements
    if agent or other_agent or rel_types:
        statements, source_counts = get_statements(
            agent=agent,
            agent_role=agent_role,
            other_agent=other_agent,
            other_role=other_role,
            stmt_sources=source_type,
            rel_types=rel_types,
            paper_term=paper_id,
            mesh_term=mesh_terms,
            limit=1000,
            evidence_limit=1000,
            return_source_counts=True,
        )
        evidence_count = {h: sum(v.values()) for h, v in source_counts.items()}
        return render_statements(
            stmts=statements,
            evidence_counts=evidence_count,
            source_counts_dict=source_counts
        )

    # Render the form page
    return render_template(
        "search/search_page.html",
        form=form,
        stmt_types_json=stmt_types_json,
    )


from flask import current_app


@search_blueprint.route("/gilda_ground", methods=["GET", "POST"])
@jwt_required(optional=True)
def gilda_ground_endpoint():
    data = request.get_json()
    current_app.logger.info(f"Received payload: {data}")
    agent_text = data.get("agent")
    if not agent_text:
        return {"error": "Agent text is required."}, 400

    try:
        gilda_list = gilda_ground(agent_text)
        return jsonify(gilda_list)
    except Exception as e:
        return {"error": str(e)}, 500

def is_valid_curie(namespace, identifier, validator):
    """Check if a given namespace is valid"""
    try:
        assert_valid_id(namespace, identifier, validator=validator)
        return True
    except ValueError:
        return False

def check_and_convert(text):
    if ":" in text:
        validator = BioregistryValidator()
        curie_validate_namespace, curie_validate_id = text.split(":", 1)
        if is_valid_curie(curie_validate_namespace, curie_validate_id, validator):
            result = (curie_validate_namespace, curie_validate_id)
            return result, True
    return text, False

def gilda_ground(agent_text):
    try:
        from gilda.api import ground
        return [r.to_json() for r in ground(agent_text)]
    except ImportError:
        import requests
        res = requests.post('http://grounding.indra.bio/ground', json={'text': agent_text})
        return res.json()
    except Exception as e:
        return {"error": f"Grounding failed: {str(e)}"}


@autoclient()
def get_ora_statements(
    target_id: str,
    genes: List[str],
    minimum_belief: float = 0.0,
    minimum_evidence: Optional[int] = None,
    is_downstream: bool = False,
    remove_medscan: bool = True,
    *,
    client: Neo4jClient,
) -> Tuple[List[Statement], Mapping[int, int]]:
    """Get statements connecting input genes to target entity for ORA analysis.

    Parameters
    ----------
    target_id : str
        The ID of the target entity (e.g., 'GO:0006955', 'MESH:D007239')
    genes : List[str]
        List of gene IDs (e.g., ['HGNC:6019', 'HGNC:11876'])
    minimum_belief : float
        Minimum belief score for relationships
    minimum_evidence : Optional[int]
        Minimum number of evidences required for a statement to be included
    is_downstream : bool
        Whether this is a downstream analysis
    remove_medscan : bool
        Whether to remove MedScan evidence from the results
    client : Neo4jClient
        The Neo4j client to use for querying

    Returns
    -------
    :
        A tuple containing:
        - List of INDRA statements representing the relationships
        - Dictionary mapping statement hashes to their evidence counts
    """
    # Normalize gene IDs
    normalized_genes = [norm_id('HGNC', gene.split(':')[1]) for gene in genes]
    print(f"DEBUG: Normalized genes: {normalized_genes[:5]}...")

    # Handle different entity types and their relationships
    namespace = target_id.split(':')[0].lower()
    id_part = target_id.split(':')[1]

    if namespace == 'mesh':
        normalized_target = f"mesh:{id_part}"
        rel_types = ["indra_rel", "has_indication"]
    elif namespace == 'fplx':
        normalized_target = f"fplx:{id_part}"
        rel_types = ["indra_rel", "isa"]
    else:
        normalized_target = target_id.lower()
        rel_types = ["indra_rel"]

    # Main query for getting statements
    query = """
    MATCH p = (d:BioEntity {id: $target_id})-[r]->(u:BioEntity)
    WHERE type(r) IN $rel_types
    AND u.id STARTS WITH "hgnc"
    AND NOT u.obsolete
    AND u.id IN $genes
    AND (type(r) <> 'indra_rel' OR r.belief > $minimum_belief)
    WITH distinct r.stmt_hash AS hash, collect(p) as pp
    RETURN pp
    """

    if is_downstream:
        query = """
        MATCH p = (u:BioEntity)-[r]->(d:BioEntity {id: $target_id})
        WHERE type(r) IN $rel_types
        AND u.id STARTS WITH "hgnc"
        AND NOT u.obsolete
        AND u.id IN $genes
        AND (type(r) <> 'indra_rel' OR r.belief > $minimum_belief)
        WITH distinct r.stmt_hash AS hash, collect(p) as pp
        RETURN pp
        """

    params = {
        "target_id": normalized_target,
        "genes": normalized_genes,
        "rel_types": rel_types,
        "minimum_belief": minimum_belief
    }
    results = client.query_tx(query, **params)
    flattened_rels = [client.neo4j_to_relation(i[0]) for rel in results for i in rel]

    # Filter relations based on minimum_evidence
    if minimum_evidence:
        flattened_rels = [
            rel for rel in flattened_rels
            if rel.data.get("evidence_count", 0) >= minimum_evidence
        ]

    stmts = indra_stmts_from_relations(flattened_rels, deduplicate=True)

    # Enrich statements with complete evidence (no limit)
    stmts = enrich_statements(
        stmts,
        client=client,
        remove_medscan=remove_medscan,
    )

    # Create evidence count mapping
    evidence_counts = {
        stmt.get_hash(): rel.data.get("evidence_count", 0)
        for rel, stmt in zip(flattened_rels, stmts)
    }

    return stmts, evidence_counts


@autoclient()
def get_network_for_statements(
    *,
    client: Neo4jClient,
) -> Dict:
    """Generate network visualization data for ORA statements from session data.

    Parameters
    ----------
    client : Neo4jClient
        The Neo4j client.

    Returns
    -------
    Dict
        A dictionary containing nodes and edges in vis.js format for network visualization.
    """
    try:
        # Get statement hashes and parameters from session
        statement_hashes = session.get('ora_statement_hashes')
        params = session.get('ora_params', {})

        if not statement_hashes or not params:
            return {"nodes": [], "edges": [], "error": "No ORA statement data found in session"}

        # Extract parameters from session
        target_id = params.get('target_id')
        genes = params.get('genes', [])
        minimum_belief = params.get('minimum_belief', 0.0)
        minimum_evidence = params.get('minimum_evidence', 2)
        is_downstream = params.get('is_downstream', False)
        regulator_type = params.get('regulator_type')
        evidence_counts = params.get('evidence_counts', {})

        # Retrieve statements by hash
        statements = get_stmts_for_stmt_hashes(
            statement_hashes,
            client=client,
            return_evidence_counts=False
        )

        if not statements:
            return {"nodes": [], "edges": []}

        logger.debug(f"Network visualization - Retrieved {len(statements)} statements from session")

        # Initialize nodes and edges collections
        nodes, edges = [], []
        node_ids = set()

        # Normalize target_id format
        if isinstance(target_id, list) and len(target_id) == 2:
            namespace, id_part = target_id
            target_id_str = f"{namespace}:{id_part}"
        elif isinstance(target_id, str) and ':' in target_id:
            target_id_str = target_id
            namespace, id_part = target_id_str.split(':', 1)
            namespace = namespace.lower()
        else:
            return {"nodes": [], "edges": [], "error": "Invalid or missing target_id in session parameters"}

        # Collect all genes that appear in the statements
        statement_entities = set()
        entity_info_dict = {}

        # Define color and shape maps for different entity types
        color_map = {
            "go": "#2196F3",  # Blue
            "mesh": "#9C27B0",  # Purple
            "wikipathways": "#FF9800",  # Orange
            "reactome": "#FF9800",  # Orange
            "hgnc": "#4CAF50",  # Green - for genes
            "default": "#607D8B"  # Gray - for other entities
        }

        shape_map = {
            "go": "hexagon",
            "mesh": "triangle",
            "wikipathways": "diamond",
            "reactome": "diamond",
            "hgnc": "box",  # Box for genes
            "default": "ellipse"  # Default shape
        }

        # Process all statements to extract connected genes
        for stmt in statements:
            for agent in stmt.agent_list():
                if agent and hasattr(agent, 'name'):
                    # Get ID in our standard format
                    entity_id = None
                    entity_type = "default"

                    # Handle gene entities
                    if 'HGNC' in agent.db_refs:
                        hgnc_id = f"HGNC:{agent.db_refs['HGNC']}"
                        # Check if this gene is in our input list
                        for gene in genes:
                            gene_id = gene if isinstance(gene, str) else f"{gene[0]}:{gene[1]}"
                            if gene_id.upper() == hgnc_id.upper():
                                entity_id = hgnc_id
                                entity_type = "hgnc"
                                break

                    if entity_id:
                        statement_entities.add(entity_id)
                        entity_info_dict[entity_id] = {
                            'name': agent.name,
                            'db_refs': getattr(agent, 'db_refs', {}),
                            'type': entity_type
                        }

        # Get target node name/symbol if available
        target_name = id_part
        if namespace.lower() == 'hgnc':
            try:
                # Try to get the gene symbol from the database
                query = """
                MATCH (n:BioEntity {id: $target_id})
                RETURN n.name
                """
                result = client.query_tx(query, target_id=target_id_str.lower())
                if result and result[0][0]:
                    target_name = result[0][0]
            except Exception as e:
                logger.warning(f"Could not get target name: {e}")

        # Always add the target node
        target_node = {
            'id': target_id_str,
            'label': target_name,
            'title': target_id_str,
            'color': {'background': color_map.get(namespace.lower(), color_map['default']),
                      'border': '#37474F'},
            'shape': shape_map.get(namespace.lower(), shape_map['default']),
            'size': 45,
            'font': {'size': 26, 'color': '#000000', 'face': 'arial', 'strokeWidth': 0, 'vadjust': -40},
            'borderWidth': 3,
            'type': namespace.lower()
        }
        nodes.append(target_node)
        node_ids.add(target_id_str)

        # Add genes that appear in the statements (not showing all input genes)
        for entity_id in statement_entities:
            if entity_id not in node_ids:
                entity_info = entity_info_dict.get(entity_id, {})
                entity_ns = entity_id.split(':')[0].lower()
                entity_label = entity_info.get('name', entity_id.split(':')[-1])
                db_refs = entity_info.get('db_refs', {})
                entity_type = entity_info.get('type', entity_ns)

                node = {
                    'id': entity_id,
                    'label': entity_label,
                    'title': entity_id,
                    'color': {'background': color_map.get(entity_type, color_map['default']),
                              'border': '#37474F'},
                    'shape': shape_map.get(entity_type, shape_map['default']),
                    'size': 35,
                    'font': {'size': 22, 'color': '#000000', 'face': 'arial', 'strokeWidth': 0, 'vadjust': -40},
                    'borderWidth': 2,
                    'type': entity_type,
                    'details': db_refs
                }

                # Add specific identifiers for genes
                if entity_type == 'hgnc':
                    node['egid'] = db_refs.get('EGID', '')
                    node['hgnc'] = db_refs.get('HGNC', '')
                    node['uniprot'] = db_refs.get('UP', '')

                nodes.append(node)
                node_ids.add(entity_id)

        # Define edge styles for different statement types
        edge_styles = {
            "Activation": {"color": "#00CC00", "dashes": False},
            "Inhibition": {"color": "#FF0000", "dashes": False},
            "Phosphorylation": {"color": "#000000", "dashes": False},
            "Complex": {"color": "#0000FF", "dashes": False,
                        "arrows": {"to": {"enabled": False}, "from": {"enabled": False}}},
            "IncreaseAmount": {"color": "#00CC00", "dashes": [5, 5]},
            "DecreaseAmount": {"color": "#FF0000", "dashes": [5, 5]}
        }

        # Create edges for statements
        for edge_count, stmt in enumerate(statements):
            agents = stmt.agent_list()
            if len(agents) < 2 or None in agents:
                continue

            stmt_type = stmt.__class__.__name__
            style = edge_styles.get(stmt_type, {"color": "#999999", "dashes": False})
            arrows = style.get('arrows', {"to": {"enabled": True, "scaleFactor": 0.5}})

            # Find the gene agent in the statement
            gene_agent = None
            for agent in agents:
                if not agent or not hasattr(agent, 'db_refs'):
                    continue

                if 'HGNC' in agent.db_refs:
                    hgnc_id = f"HGNC:{agent.db_refs['HGNC']}"
                    # Check if this gene is in our input list
                    for gene in genes:
                        gene_id = gene if isinstance(gene, str) else f"{gene[0]}:{gene[1]}"
                        if gene_id.upper() == hgnc_id.upper():
                            gene_agent = agent
                            break
                    if gene_agent:
                        break

            if not gene_agent:
                continue

            # Set source and target based on direction
            source = f"HGNC:{gene_agent.db_refs['HGNC']}" if is_downstream else target_id_str
            target = target_id_str if is_downstream else f"HGNC:{gene_agent.db_refs['HGNC']}"

            # Only add the edge if both source and target nodes exist
            if source in node_ids and target in node_ids:
                # Create edge
                edges.append({
                    'id': f"e{edge_count}",
                    'from': source,
                    'to': target,
                    'title': f"{stmt_type}: {stmt}",
                    'color': {'color': style['color'], 'highlight': style['color'], 'hover': style['color']},
                    'dashes': style['dashes'],
                    'arrows': arrows,
                    'width': min(5, 1 + evidence_counts.get(str(stmt.get_hash()), 0) / 5),
                    'details': {
                        'statement_type': stmt_type,
                        'belief': getattr(stmt, 'belief', 0.5),
                        'evidence_count': evidence_counts.get(str(stmt.get_hash()), 0),
                        'indra_statement': str(stmt)
                    },
                    'label': ''
                })

        logger.debug(f"Network data - nodes: {len(nodes)}, edges: {len(edges)}")

        # Return the network visualization data
        return {'nodes': nodes, 'edges': edges}
    except Exception as e:
        logger.error(f"Error in get_network_for_statements: {str(e)}", exc_info=True)
        return {"nodes": [], "edges": [], "error": str(e)}


@search_blueprint.route("/ora_statements/", methods=['GET'])
@jwt_required(optional=True)
def search_ora_statements():
    """Endpoint to get INDRA statements connecting input genes to a target entity."""
    target_id = request.args.get("target_id")
    genes = request.args.getlist("genes")
    is_downstream = request.args.get("is_downstream", "").lower() == "true"
    regulator_type = request.args.get("regulator_type")

    # Validate required parameters
    if not target_id or not genes:
        return jsonify({"error": "target_id and genes are required"}), 400

    # Parse numeric parameters
    try:
        minimum_evidence = int(request.args.get("minimum_evidence") or 2)
        minimum_belief = float(request.args.get("minimum_belief") or 0.0)
    except (ValueError, TypeError):
        return jsonify({"error": "Invalid minimum_evidence or minimum_belief value"}), 400

    _, _, user_email = resolve_email()
    remove_medscan = not bool(user_email)

    try:
        # Get all statements
        statements, evidence_counts = get_ora_statements(
            target_id=target_id,
            genes=genes,
            minimum_belief=minimum_belief,
            minimum_evidence=minimum_evidence,
            is_downstream=is_downstream,
            remove_medscan=remove_medscan,
        )

        logger.debug(f"Total statements before filtering: {len(statements)}")

        # Filter statements based on regulator type
        if regulator_type == 'kinase':
            # Filter for phosphorylation statements
            filtered_statements = [
                stmt for stmt in statements
                if isinstance(stmt, Phosphorylation) or
                   "phosphorylation" in str(stmt).lower()
            ]
        elif regulator_type == 'tf':
            # Filter for increase/decrease amount statements
            filtered_statements = [
                stmt for stmt in statements
                if isinstance(stmt, (IncreaseAmount, DecreaseAmount)) or
                   "increases amount" in str(stmt).lower() or
                   "decreases amount" in str(stmt).lower()
            ]
        else:
            # No filtering for other cases
            filtered_statements = statements

        # Update evidence counts to match filtered statements
        filtered_evidence_counts = {
            k: v for k, v in evidence_counts.items()
            if k in [stmt.get_hash() for stmt in filtered_statements]
        }

        logger.debug(f"Statements after filtering: {len(filtered_statements)}")
        if filtered_statements:
            logger.debug(f"First filtered statement: {str(filtered_statements[0])}")

        # Store parameters and statement hashes in session for network visualization
        if filtered_statements:
            # Store statement hashes
            session['ora_statement_hashes'] = [stmt.get_hash() for stmt in filtered_statements]

            # Store other parameters needed for the network
            session['ora_params'] = {
                'target_id': target_id,
                'genes': genes,
                'minimum_belief': minimum_belief,
                'minimum_evidence': minimum_evidence,
                'is_downstream': is_downstream,
                'regulator_type': regulator_type,
                'evidence_counts': filtered_evidence_counts
                }

        # Render the statements
        return render_statements(
            stmts=filtered_statements,
            evidence_count=filtered_evidence_counts,
            store_hashes_in_session=True,
            prefix='statements',
            identifier=target_id
        )

    except Exception as e:
        logger.error(f"Error in search_ora_statements: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@autoclient()
def get_signed_statements(
    target_id: str,
    positive_genes: List[str],
    negative_genes: List[str],
    minimum_belief: float = 0.0,
    minimum_evidence: Optional[int] = None,
    *,
    client: Neo4jClient,
) -> Tuple[List[Statement], Mapping[int, int]]:
    """Get statements showing signed relationships between genes and target.

   Parameters
   ----------
   target_id : str
       The ID of the target entity
   positive_genes : List[str]
       List of gene IDs that show positive correlation with target
   negative_genes : List[str]
       List of gene IDs that show negative correlation with target
   minimum_belief : float, optional
       Minimum belief score threshold for relationships, by default 0.0
   minimum_evidence : Optional[int], optional
       Minimum number of evidences required, by default None
   client : Neo4jClient
       The Neo4j client instance

   Returns
   -------
   :
       A tuple containing:
       - List of INDRA statements representing the relationships
       - Dictionary mapping statement hashes to evidence counts
    """
    pos_genes = [norm_id('HGNC', gene.split(':')[1]) for gene in positive_genes]
    neg_genes = [norm_id('HGNC', gene.split(':')[1]) for gene in negative_genes]

    namespace = target_id.split(':')[0].lower()
    id_part = target_id.split(':')[1]

    if namespace == 'chebi':
        normalized_target = f"chebi:{id_part}"
        rel_types = ["indra_rel", "has_metabolite"]
    elif namespace == 'mesh':
        normalized_target = f"mesh:{id_part}"
        rel_types = ["indra_rel", "has_indication"]
    elif namespace == 'hgnc':
        normalized_target = f"hgnc:{id_part}"
        rel_types = ["indra_rel"]
    elif namespace == 'fplx':
        normalized_target = f"fplx:{id_part}"
        rel_types = ["indra_rel", "isa"]
    else:
        normalized_target = target_id.lower()
        rel_types = ["indra_rel"]

    query = """
            MATCH p = (target:BioEntity {id: $target_id})-[r]->(gene:BioEntity)
            WHERE type(r) IN $rel_types
            AND gene.id IN $gene_list
            AND r.belief > $minimum_belief
            AND r.evidence_count >= $minimum_evidence
            AND NOT gene.obsolete
            AND r.stmt_type IN $allowed_stmt_types
            RETURN DISTINCT p
            """

    flattened_rels = []

    if pos_genes:
        pos_params = {
            "target_id": normalized_target,
            "gene_list": pos_genes,
            "rel_types": rel_types,
            "minimum_belief": minimum_belief,
            "minimum_evidence": minimum_evidence,
            "allowed_stmt_types": [
                'IncreaseAmount',
                'Activation'
            ]
        }
        pos_results = client.query_tx(query, **pos_params)
        for result in pos_results:
            path = result[0]  # Get the Neo4j path directly
            rel = client.neo4j_to_relation(path)
            flattened_rels.append(rel)

    if neg_genes:
        neg_params = {
            "target_id": normalized_target,
            "gene_list": neg_genes,
            "rel_types": rel_types,
            "minimum_belief": minimum_belief,
            "minimum_evidence": minimum_evidence,
            "allowed_stmt_types": [
                'DecreaseAmount',
                'Inhibition'
            ]
        }
        neg_results = client.query_tx(query, **neg_params)
        for result in neg_results:
            path = result[0]  # Get the Neo4j path directly
            rel = client.neo4j_to_relation(path)
            flattened_rels.append(rel)

    stmts = indra_stmts_from_relations(flattened_rels, deduplicate=True)
    stmts = enrich_statements(stmts, client=client)

    evidence_counts = {
        stmt.get_hash(): rel.data.get("evidence_count", 0)
        for rel, stmt in zip(flattened_rels, stmts)
    }

    return stmts, evidence_counts


@search_blueprint.route("/signed_statements/", methods=['GET'])
@jwt_required(optional=True)
def search_signed_statements():
    target_id = request.args.get("target_id")
    positive_genes = request.args.getlist("positive_genes")
    negative_genes = request.args.getlist("negative_genes")
    minimum_evidence = request.args.get("minimum_evidence", "1")
    minimum_belief = request.args.get("minimum_belief", "0.0")

    try:
        minimum_evidence = int(minimum_evidence)
        minimum_belief = float(minimum_belief)
    except (ValueError, TypeError) as e:
        return jsonify({"error": "Invalid parameter values"}), 400

    if not target_id or (not positive_genes and not negative_genes):
        return jsonify({"error": "target_id and at least one gene list required"}), 400

    statements, evidence_counts = get_signed_statements(
        target_id=target_id,
        positive_genes=positive_genes,
        negative_genes=negative_genes,
        minimum_belief=minimum_belief,
        minimum_evidence=minimum_evidence
    )

    return render_statements(
        stmts=statements,
        evidence_count=evidence_counts
    )


@autoclient()
def get_continuous_statements(
    target_id: str,
    gene_names: List[str],
    source: str,
    minimum_belief: float = 0.0,
    minimum_evidence: Optional[int] = None,
    *,
    client: Neo4jClient,
) -> Tuple[List[Statement], Mapping[int, int]]:
    """Get statements for continuous analysis.

    Parameters
    ----------
    target_id : str
        The ID of the target entity
    gene_names : List[str]
        List of gene names
    source : str
        Type of analysis ('indra-upstream' or 'indra-downstream')
    minimum_belief : float
        Minimum belief score for relationships
    minimum_evidence : Optional[int]
        Minimum number of evidences required
    client : Neo4jClient
        The Neo4j client to use for querying
    Returns
    -------
    :
        A tuple containing:
        - List of INDRA statements representing the relationships
        - Dictionary mapping statement hashes to their evidence counts
    """
    prefix, entity = target_id.split(':', 1)
    if prefix.lower() in ['fplx', 'mesh']:
        normalized_target = f"{prefix.lower()}:{entity}"
    else:
        normalized_target = target_id.lower()

    if source == 'indra-upstream':
        query = """
                MATCH path=(gene:BioEntity)-[r:indra_rel]-(target:BioEntity)
                WHERE target.id = $target_id
                AND gene.name IN $gene_names
                AND r.belief > $minimum_belief
                AND r.evidence_count >= $minimum_evidence
                RETURN path
                """
    elif source == 'indra-downstream':
        query = """
                MATCH path=(regulator:BioEntity)-[r:indra_rel]-(gene:BioEntity)
                WHERE regulator.id = $target_id
                AND gene.name IN $gene_names
                AND r.belief > $minimum_belief
                AND r.evidence_count >= $minimum_evidence
                RETURN path
                """

    params = {
        "target_id": normalized_target,
        "gene_names": gene_names,
        "minimum_belief": minimum_belief,
        "minimum_evidence": minimum_evidence if minimum_evidence is not None else 0
    }

    results = client.query_tx(query, **params)

    all_relations = []
    for result in results:
        try:
            rels = client.neo4j_to_relations(result[0])
            all_relations.extend(rels)
        except Exception:
            continue

    if not all_relations:
        return [], {}

    stmts = indra_stmts_from_relations(all_relations, deduplicate=True)
    stmts = enrich_statements(stmts, client=client)

    evidence_counts = {
        stmt.get_hash(): rel.data.get("evidence_count", 0)
        for rel, stmt in zip(all_relations, stmts)
    }

    return stmts, evidence_counts


@search_blueprint.route("/continuous_statements/", methods=['GET'])
@jwt_required(optional=True)
def search_continuous_statements():
    """Endpoint to get INDRA statements for continuous analysis results."""
    target_id = request.args.get("target_id")
    genes = request.args.getlist("genes")
    source = request.args.get("source", "indra-upstream")

    try:
        minimum_evidence = int(request.args.get("minimum_evidence") or 1)
        minimum_belief = float(request.args.get("minimum_belief") or 0.0)
    except (ValueError, TypeError):
        return jsonify({"error": "Invalid parameter values"}), 400

    if not target_id or not genes:
        return jsonify({"error": "target_id and genes are required"}), 400

    try:
        statements, evidence_counts = get_continuous_statements(
            target_id=target_id,
            gene_names=genes,
            source=source,
            minimum_belief=minimum_belief,
            minimum_evidence=minimum_evidence
        )

        return render_statements(
            stmts=statements,
            evidence_count=evidence_counts
        )

    except ValueError as e:
        return jsonify({"error": str(e)}), 400


@autoclient()
def get_kinase_phosphosite_statements(
    kinase_id: str,
    phosphosites: List[str],
    minimum_belief: float = 0.0,
    minimum_evidence: Optional[int] = None,
    *,
    client: Neo4jClient,
) -> Tuple[List[Statement], Mapping[int, int]]:
    """Get statements connecting a kinase to phosphosites.

    Parameters
    ----------
    kinase_id : str
        The ID of the kinase (e.g., 'hgnc:1722', 'fplx:MAPK')
    phosphosites : List[str]
        List of phosphosites in the format "gene-site" (e.g., "MAPK1-T202") or
        "uniprot-site" (e.g., "P28482-T202")
    minimum_belief : float
        Minimum belief score for relationships
    minimum_evidence : Optional[int]
        Minimum number of evidences required
    client : Neo4jClient
        The Neo4j client to use for querying

    Returns
    -------
    :
        A tuple containing:
        - List of INDRA statements representing the relationships
        - Dictionary mapping statement hashes to their evidence counts
    """
    # Normalize kinase ID
    namespace = kinase_id.split(':')[0].lower()
    id_part = kinase_id.split(':')[1]

    if namespace == 'fplx':
        normalized_kinase = f"fplx:{id_part}"
    elif namespace == 'hgnc':
        normalized_kinase = f"hgnc:{id_part}"
    else:
        normalized_kinase = kinase_id.lower()

    # Parse phosphosites and convert UniProt IDs if needed
    raw_phosphosites = [tuple(site.split("-")) for site in phosphosites if "-" in site]
    processed_phosphosites, errors = parse_phosphosite_list(raw_phosphosites)

    if errors:
        logger.debug(f"Some phosphosites could not be parsed: {errors}")

    gene_names = [gene for gene, _ in processed_phosphosites]

    if not gene_names:
        return [], {}

    # Find phosphorylation relationships between the kinase and these genes
    query = """
    MATCH path = (kinase:BioEntity)-[r:indra_rel]->(substrate:BioEntity)
    WHERE kinase.id = $kinase_id
      AND substrate.name IN $gene_names
      AND r.stmt_type = 'Phosphorylation'
      AND r.belief >= $minimum_belief
      AND r.evidence_count >= $minimum_evidence
    RETURN path
    """

    params = {
        "kinase_id": normalized_kinase,
        "gene_names": gene_names,
        "minimum_belief": minimum_belief,
        "minimum_evidence": minimum_evidence if minimum_evidence is not None else 0
    }

    results = client.query_tx(query, **params)

    # Process results
    all_relations = []
    if results:
        for result in results:
            try:
                rel = client.neo4j_to_relation(result[0])
                all_relations.append(rel)
            except Exception as e:
                logger.error(f"Error processing relation: {e}")
                continue

    if not all_relations:
        return [], {}

    # Convert to INDRA statements
    stmts = indra_stmts_from_relations(all_relations, deduplicate=True)
    stmts = enrich_statements(stmts, client=client)

    # Create evidence count mapping
    evidence_counts = {
        stmt.get_hash(): rel.data.get("evidence_count", 0)
        for rel, stmt in zip(all_relations, stmts)
    }

    return stmts, evidence_counts


@search_blueprint.route("/kinase_statements/", methods=['GET'])
@jwt_required(optional=True)
def search_kinase_statements():
    """Endpoint to get INDRA statements connecting kinases to phosphosites."""
    kinase_id = request.args.get("kinase_id")
    phosphosites = request.args.getlist("phosphosites")

    try:
        minimum_evidence = int(request.args.get("minimum_evidence") or 1)
        minimum_belief = float(request.args.get("minimum_belief") or 0.0)
    except (ValueError, TypeError):
        return jsonify({"error": "Invalid parameter values"}), 400

    if not kinase_id:
        return jsonify({"error": "kinase_id is required"}), 400

    if not phosphosites:
        return jsonify({"error": "No phosphosites provided"}), 400

    try:
        statements, evidence_counts = get_kinase_phosphosite_statements(
            kinase_id=kinase_id,
            phosphosites=phosphosites,
            minimum_belief=minimum_belief,
            minimum_evidence=minimum_evidence
        )

        return render_statements(
            stmts=statements,
            evidence_count=evidence_counts
        )

    except Exception as e:
        logger.error(f"Error in get_kinase_phosphosite_statements: {str(e)}")
        return jsonify({"error": str(e)}), 500
