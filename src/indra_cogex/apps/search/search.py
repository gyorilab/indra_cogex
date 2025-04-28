import json
from typing import List, Optional, Mapping, Tuple, Dict
import logging

from flask import Blueprint, render_template, request, jsonify, redirect, url_for
from flask_jwt_extended import jwt_required
from flask_wtf import FlaskForm
from indra.statements import get_all_descendants, Statement, Phosphorylation, IncreaseAmount, DecreaseAmount
from indra.statements.validate import assert_valid_id, BioregistryValidator
from wtforms import StringField, SubmitField
from wtforms.fields.simple import BooleanField
from wtforms.validators import DataRequired

from indra_cogex.analysis.gene_analysis import parse_phosphosite_list
from indra_cogex.apps.utils import render_statements
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

        if agent:
            agent, is_curie = check_and_convert(agent)
            agent_exists = check_agent_existence(agent)

            if agent_exists is False:
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
            "agent_tuple": request.form.get("agent_tuple"),
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

    #Check if the agent is in CURIE form
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
        client=client
    )

    # Create evidence count mapping
    evidence_counts = {
        stmt.get_hash(): rel.data.get("evidence_count", 0)
        for rel, stmt in zip(flattened_rels, stmts)
    }

    return stmts, evidence_counts


@autoclient()
def get_network_for_statements(
    target_id: str,
    genes: List[str],
    minimum_belief: float = 0.0,
    minimum_evidence: int = 2,
    is_downstream: bool = False,
    regulator_type: Optional[str] = None,
    limit: int = 50,
    *,
    client: Neo4jClient,
) -> Dict:
    """Generate network visualization data for ORA statements.

    Parameters
    ----------
    target_id : str
        The ID of the target entity (e.g., 'GO:0006955', 'MESH:D007239')
    genes : List[str]
        List of gene IDs (e.g., ['HGNC:6019', 'HGNC:11876'])
    minimum_belief : float
        Minimum belief score for relationships
    minimum_evidence : int
        Minimum number of evidences required for a statement to be included
    is_downstream : bool
        Whether this is a downstream analysis
    regulator_type : Optional[str]
        Type of regulator to filter for ('kinase', 'tf', or None)
    limit : int
        Maximum number of statements to include in the network
    client : Neo4jClient
        The Neo4j client

    Returns
    -------
    Dict
        A dictionary containing nodes and edges in vis.js format for network visualization.
    """
    try:
        # Get statements connecting genes to target
        statements, evidence_counts = get_ora_statements(
            target_id=target_id,
            genes=genes,
            minimum_belief=minimum_belief,
            minimum_evidence=minimum_evidence,
            is_downstream=is_downstream,
            client=client
        )

        # Filter statements based on regulator type
        if regulator_type == 'kinase':
            # Filter for phosphorylation statements
            filtered_statements = [
                stmt for stmt in statements
                if "phosphorylation" in str(stmt).lower()
            ]
        elif regulator_type == 'tf':
            # Filter for increase/decrease amount statements
            filtered_statements = [
                stmt for stmt in statements
                if "increases amount" in str(stmt).lower() or
                   "decreases amount" in str(stmt).lower()
            ]
        else:
            # No filtering for other cases
            filtered_statements = statements

        # Sort statements by evidence count and limit
        sorted_statements = sorted(
            filtered_statements,
            key=lambda stmt: evidence_counts.get(stmt.get_hash(), 0),
            reverse=True
        )[:limit]

        if not sorted_statements:
            return {"nodes": [], "edges": []}

        # Create nodes and edges for the network
        nodes = []
        edges = []
        edge_count = 0
        node_ids = set()

        # Create a mapping of HGNC IDs to gene symbols from statements
        hgnc_id_to_symbol = {}

        # First pass: extract gene symbols from statements
        for stmt in sorted_statements:
            agents = stmt.agent_list()
            for agent in agents:
                if agent is None:
                    continue

                if 'HGNC' in agent.db_refs and hasattr(agent, 'name'):
                    hgnc_id = str(agent.db_refs['HGNC'])
                    hgnc_id_to_symbol[hgnc_id] = agent.name

        # Extract target information
        namespace, id_part = target_id.split(':')
        target_node_id = target_id

        # Set appropriate style for target node based on namespace
        target_shape = "ellipse"
        target_color = "#607D8B"  # Default gray-blue

        if namespace.lower() == "go":
            target_shape = "hexagon"
            target_color = "#2196F3"  # blue
        elif namespace.lower() == "mesh":
            target_shape = "triangle"
            target_color = "#9C27B0"  # purple
        elif namespace.lower() in ["wikipathways", "reactome"]:
            target_shape = "diamond"
            target_color = "#FF9800"  # orange

        # Add target node
        nodes.append({
            'id': str(target_node_id),
            'label': str(id_part),
            'title': f"{target_id}",
            'color': {
                'background': target_color,
                'border': '#37474F'
            },
            'shape': target_shape,
            'size': 45,  # Make target larger
            'font': {
                'size': 26,
                'color': '#000000',
                'face': 'arial',
                'strokeWidth': 0,
                'vadjust': -40
            },
            'borderWidth': 3,
            'type': namespace.lower()
        })
        node_ids.add(target_node_id)

        # Add nodes for all genes
        for gene_id in genes:
            # Extract HGNC ID number from gene_id
            if gene_id.startswith('HGNC:'):
                hgnc_id = gene_id.split(':')[-1]
                # Use the symbol from our mapping if available, else use the ID
                gene_label = hgnc_id_to_symbol.get(hgnc_id, hgnc_id)
            else:
                # If not an HGNC ID, just use the ID itself
                gene_label = gene_id.split(':')[-1]

            if gene_id not in node_ids:
                nodes.append({
                    'id': str(gene_id),
                    'label': str(gene_label),  # Use symbol instead of ID
                    'title': f"{gene_id}",
                    'color': {
                        'background': '#4CAF50',  # green
                        'border': '#37474F'
                    },
                    'shape': 'box',
                    'size': 35,
                    'font': {
                        'size': 22,
                        'color': '#000000',
                        'face': 'arial',
                        'strokeWidth': 0,
                        'vadjust': -40
                    },
                    'borderWidth': 2,
                    'type': 'gene'
                })
                node_ids.add(gene_id)

        # Process statements to create edges
        for stmt in sorted_statements:
            # Extract agent information
            agents = stmt.agent_list()
            if len(agents) < 2 or None in agents:
                continue

            # Get statement type and determine edge style
            stmt_type = stmt.__class__.__name__

            # Determine edge color and style based on statement type
            edge_color = "#999999"  # Default gray
            dashes = False
            arrows = {"to": {"enabled": True, "scaleFactor": 0.5}}

            if "Activation" in stmt_type:
                edge_color = '#00CC00'  # Green
            elif "Inhibition" in stmt_type:
                edge_color = '#FF0000'  # Red
            elif "Phosphorylation" in stmt_type:
                edge_color = '#000000'  # Black
            elif "Complex" in stmt_type:
                edge_color = '#0000FF'  # Blue
                arrows = {"to": {"enabled": False}, "from": {"enabled": False}}
            elif "IncreaseAmount" in stmt_type:
                edge_color = '#00CC00'  # Green
                dashes = [5, 5]
            elif "DecreaseAmount" in stmt_type:
                edge_color = '#FF0000'  # Red
                dashes = [5, 5]

            # Find the gene involved in this statement
            gene_agent = None
            for agent in agents:
                if agent is None:
                    continue

                # Look for HGNC reference
                if 'HGNC' in agent.db_refs:
                    gene_id = f"HGNC:{agent.db_refs['HGNC']}"
                    if gene_id in genes:
                        gene_agent = agent
                        break

            if gene_agent is None:
                continue

            # Create edge between gene and target
            if is_downstream:
                source = f"HGNC:{gene_agent.db_refs['HGNC']}"
                target = target_node_id
            else:
                source = target_node_id
                target = f"HGNC:{gene_agent.db_refs['HGNC']}"

            # Get evidence count
            evidence_count = evidence_counts.get(stmt.get_hash(), 0)
            belief = getattr(stmt, 'belief', 0.5)

            edges.append({
                'id': f"e{edge_count}",
                'from': str(source),
                'to': str(target),
                'title': f"{stmt_type}: {str(stmt)}",
                'color': {
                    'color': edge_color,
                    'highlight': edge_color,
                    'hover': edge_color
                },
                'dashes': dashes,
                'arrows': arrows,
                'width': min(5, 1 + evidence_count / 5),  # Scale width based on evidence
                'details': {
                    'statement_type': stmt_type,
                    'belief': belief,
                    'evidence_count': evidence_count,
                    'indra_statement': str(stmt)
                },
                'label': ''
            })
            edge_count += 1

        return {
            'nodes': nodes,
            'edges': edges
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
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

    try:
        # Get all statements
        statements, evidence_counts = get_ora_statements(
            target_id=target_id,
            genes=genes,
            minimum_belief=minimum_belief,
            minimum_evidence=minimum_evidence,
            is_downstream=is_downstream
        )

        logger.debug(f"Total statements before filtering: {len(statements)}")

        # Filter statements based on regulator type
        if regulator_type == 'kinase':
            # Filter for phosphorylation statements
            filtered_statements = [
                stmt for stmt in statements
                if "phosphorylation" in str(stmt).lower()
            ]
        elif regulator_type == 'tf':
            # Filter for increase/decrease amount statements
            filtered_statements = [
                stmt for stmt in statements
                if "increases amount" in str(stmt).lower() or
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

        # Render the statements with network visualization parameters
        return render_statements(
            stmts=filtered_statements,
            evidence_count=filtered_evidence_counts,
            prefix="statements",
            identifier=target_id,
            genes=genes,
            is_downstream=is_downstream,
            regulator_type=regulator_type,
            minimum_belief=minimum_belief,
            minimum_evidence=minimum_evidence
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
        MATCH p = (gene:BioEntity)-[r]-(target:BioEntity)
        WHERE target.id = $target_id
        AND type(r) IN $rel_types
        AND gene.id IN $gene_list
        AND r.belief > $minimum_belief
        AND r.evidence_count >= $minimum_evidence
        AND NOT gene.obsolete
        AND (
            (startNode(r) = gene AND r.stmt_type IN $stmt_types_gene_to_target)
            OR
            (startNode(r) = target AND r.stmt_type IN $stmt_types_target_to_gene)
        )
        RETURN p
        """

    flattened_rels = []

    if pos_genes:
        pos_params = {
            "target_id": normalized_target,
            "gene_list": pos_genes,
            "rel_types": rel_types,
            "minimum_belief": minimum_belief,
            "minimum_evidence": minimum_evidence,
            "stmt_types_gene_to_target": [
                'DecreaseAmount',
                'Inhibition'
            ],
            "stmt_types_target_to_gene": [
                'IncreaseAmount',
                'Activation',
                'Complex'
            ]
        }
        pos_results = client.query_tx(query, **pos_params)
        for result in pos_results:
            path = result[0]
            rel = client.neo4j_to_relation(path)
            flattened_rels.append(rel)

    if neg_genes:
        neg_params = {
            "target_id": normalized_target,
            "gene_list": neg_genes,
            "rel_types": rel_types,
            "minimum_belief": minimum_belief,
            "minimum_evidence": minimum_evidence,
            "stmt_types_gene_to_target": [
                'IncreaseAmount',
                'Activation'
            ],
            "stmt_types_target_to_gene": [
                'DecreaseAmount',
                'Inhibition'
            ]
        }
        neg_results = client.query_tx(query, **neg_params)
        for result in neg_results:
            path = result[0]
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
