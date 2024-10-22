"""
This module provides Flask-RESTful APIs for querying bioentity information.

Namespaces:
-----------
bioentity_ns : flask_restx.Namespace
    Namespace for BioEntity related queries.

Models:
----------
uniprot_mnemonic_ids_model : flask_restx.Model
    Model for a list of UniProt mnemonic IDs.
uniprot_ids_model : flask_restx.Model
    Model for a list of UniProt IDs.
hgnc_ids_model : flask_restx.Model
    Model for a list of HGNC IDs.
kinase_genes_model : flask_restx.Model
    Model for a list of kinase gene names.
phosphatase_genes_model : flask_restx.Model
    Model for a list of phosphatase gene names.
transcription_factor_genes_model : flask_restx.Model
    Model for a list of transcription factor gene names.

Resources:
----------
UniprotIdsFromUniprotMnemonicIds : flask_restx.Resource
    Resource for converting UniProt mnemonic IDs to UniProt IDs.
HgncIdsFromUniprotIds : flask_restx.Resource
    Resource for mapping UniProt IDs to HGNC IDs.
HgncNamesFromHgncIds : flask_restx.Resource
    Resource for retrieving HGNC names from HGNC IDs.
CheckIfKinase : flask_restx.Resource
    Resource for checking if given genes are kinases.
CheckIfPhosphatase : flask_restx.Resource
    Resource for checking if given genes are phosphatases.
CheckIfTranscriptionFactor : flask_restx.Resource
    Resource for checking if given genes are transcription factors.
"""

from flask import request
from flask_restx import Namespace, Resource, fields

from indra.databases import hgnc_client, uniprot_client

bioentity_ns = Namespace("BioEntity", description="Queries for BioEntity", path="/api")

uniprot_mnemonic_ids_model = bioentity_ns.model(
    'get_uniprot_ids_from_uniprot_mnemonic_ids_model',
    {
        'uniprot_mnemonic_ids': fields.List(
            fields.String,
            required=True,
            description='List of UniProt mnemonic IDs', example=['CLH1_HUMAN']
        )
    }
)
uniprot_ids_model = bioentity_ns.model(
    'get_hgnc_ids_from_uniprot_ids_model',
    {
        'uniprot_ids': fields.List(
            fields.String,
            required=True,
            description='List of UniProt IDs',
            example=['Q00610']
        )
    }
)
hgnc_ids_model = bioentity_ns.model(
    'get_hgnc_names_from_hgnc_ids_model',
    {
        'hgnc_ids': fields.List(
            fields.String,
            required=True,
            description='List of HGNC IDs',
            example=['2092']
        )
    }
)
kinase_genes_model = bioentity_ns.model(
    'get_is_kinase_model',
    {
        'genes': fields.List(
            fields.String,
            required=True,
            description='List of gene names',
            example=['CHEK1']
        )
    }
)
phosphatase_genes_model = bioentity_ns.model(
    'get_is_phosphatase_model',
    {
        'genes': fields.List(
            fields.String,
            required=True,
            description='List of gene names',
            example=['MTM1']
        )
    }
)
transcription_factor_genes_model = bioentity_ns.model(
    'get_is_transcription_factor_model',
    {
        'genes': fields.List(
            fields.String,
            required=True,
            description='List of gene names',
            example=['STAT1']
        )
    }
)


@bioentity_ns.expect(uniprot_mnemonic_ids_model)
@bioentity_ns.route("/get_uniprot_ids_from_uniprot_mnemonic_ids")
class UniprotIdsFromUniprotMnemonicIds(Resource):
    """A Flask-RESTful resource for converting UniProt mnemonic IDs to UniProt IDs.

    Methods
    -------
    post()
        Receives a JSON payload with a list of UniProt mnemonic IDs and returns a
        mapping of these IDs to their corresponding UniProt IDs.
    """

    def post(self):
        """Maps UniProt mnemonic IDs to UniProt IDs.

        This method expects a JSON payload with a key "uniprot_mnemonic_ids"
        containing a list of UniProt mnemonic IDs. It uses the UniProt client to
        convert each mnemonic ID to a UniProt ID. Then, it adds the mapping from the
        mnemonic ID to the UniProt ID to the result.

        Returns
        -------
        :
            A dictionary mapping UniProt mnemonic IDs to UniProt IDs.
        """
        uniprot_mnemonic_ids = request.json["uniprot_mnemonic_ids"]
        mapping = dict()
        for uniprot_mnemonic_id in uniprot_mnemonic_ids:
            uniprot_id = uniprot_client.get_id_from_mnemonic(uniprot_mnemonic_id)
            if uniprot_id:
                mapping[uniprot_mnemonic_id] = uniprot_id
        return mapping


@bioentity_ns.expect(uniprot_ids_model)
@bioentity_ns.route("/get_hgnc_ids_from_uniprot_ids")
class HgncIdsFromUniprotIds(Resource):
    """A Flask-RESTful resource to map UniProt IDs to HGNC IDs.

    Methods
    -------
    post()
        Receives a JSON payload containing a list of UniProt IDs and returns a mapping
        of UniProt IDs to HGNC IDs.
    """

    def post(self):
        """Maps UniProt IDs to HGNC IDs.

        This method expects a JSON payload with a list of UniProt IDs under the key
        "uniprot_ids". It returns a dictionary mapping each UniProt ID to its
        corresponding HGNC ID, if available.

        Returns
        -------
        :
            A dictionary where keys are UniProt IDs and values are HGNC IDs.
        """
        uniprot_ids = request.json["uniprot_ids"]
        mapping = dict()
        for uniprot_id in uniprot_ids:
            hgnc_id = uniprot_client.get_hgnc_id(uniprot_id)
            if hgnc_id:
                mapping[uniprot_id] = hgnc_id
        return mapping


@bioentity_ns.expect(hgnc_ids_model)
@bioentity_ns.route("/get_hgnc_names_from_hgnc_ids")
class HgncNamesFromHgncIds(Resource):
    """A Flask-RESTful resource for retrieving HGNC names from HGNC IDs.

    Methods
    -------
    post()
        Receives a JSON payload containing a list of HGNC IDs and returns a
        dictionary mapping each HGNC ID to its corresponding HGNC name.
    """

    def post(self):
        """Maps HGNC IDs to their corresponding names.

        This method expects a JSON payload with a list of HGNC IDs under the key
        "hgnc_ids". It retrieves the corresponding HGNC names using the `hgnc_client`
        and returns a dictionary mapping each HGNC ID to its name.

        Returns
        -------
        :
            A dictionary where keys are HGNC IDs and values are HGNC names.
        """
        hgnc_ids = request.json["hgnc_ids"]
        mapping = dict()
        for hgnc_id in hgnc_ids:
            hgnc_name = hgnc_client.get_hgnc_name(hgnc_id)
            if hgnc_name:
                mapping[hgnc_id] = hgnc_name
        return mapping


@bioentity_ns.expect(kinase_genes_model)
@bioentity_ns.route("/is_kinase")
class CheckIfKinase(Resource):
    """A Flask-RESTful resource that checks if given genes are kinases.

    Methods
    -------
    post()
        Receives a JSON payload with a list of genes and returns a mapping of each
        gene to a boolean indicating if it is a kinase.
    """

    def post(self):
        """Determines if the given genes are kinases.

        This method expects a JSON payload with a list of genes under the key "genes".
        It checks each gene to determine if it is a kinase using the
        `hgnc_client.is_kinase` method and returns a mapping of each gene to a boolean
        indicating whether it is a kinase.

        Returns
        -------
        :
            A dictionary where the keys are gene names and the values are booleans
            indicating whether each gene is a kinase.
        """
        genes = request.json["genes"]
        mapping = dict()
        for gene in genes:
            gene_is_kinase = hgnc_client.is_kinase(gene)
            mapping[gene] = gene_is_kinase
        return mapping


@bioentity_ns.expect(phosphatase_genes_model)
@bioentity_ns.route("/is_phosphatase")
class CheckIfPhosphatase(Resource):
    """A Flask-RESTful resource for checking if a list of genes are phosphatases.

    Methods
    -------
    post()
        Receives a JSON payload with a list of genes and returns a mapping of each
        gene to a boolean indicating if it is a phosphatase.
    """

    def post(self):
        """Determines if the given genes are phosphatases.

        This method expects a JSON payload with a list of gene identifiers under the
        key "genes". It checks each gene to determine if it is a phosphatase using the
        `hgnc_client.is_phosphatase` method. The result is a dictionary mapping each
        gene to a boolean indicating whether it is a phosphatase.

        Returns
        -------
        :
            A dictionary where keys are gene identifiers and values are booleans
            indicating if the gene is a phosphatase.
        """
        genes = request.json["genes"]
        mapping = dict()
        for gene in genes:
            gene_is_phosphate = hgnc_client.is_phosphatase(gene)
            mapping[gene] = gene_is_phosphate
        return mapping


@bioentity_ns.expect(transcription_factor_genes_model)
@bioentity_ns.route("/is_transcription_factor")
class CheckIfTranscriptionFactor(Resource):
    """A Flask-RESTful resource that checks if given genes are transcription factors.

    Methods
    -------
    post()
        Receives a JSON payload with a list of genes and returns a mapping of each
        gene to a boolean indicating whether it is a transcription factor.
    """

    def post(self):
        """Determines if the given genes are transcription factors.

        This method expects a JSON payload with a list of genes under the key "genes".
        It checks each gene to determine if it is a transcription factor using the
        `hgnc_client.is_transcription_factor` method and returns a mapping of genes
        to their transcription factor status.

        Returns
        -------
        :
            A dictionary where keys are gene names and values are booleans indicating
            whether the gene is a transcription factor.
        """
        genes = request.json["genes"]
        mapping = dict()
        for gene in genes:
            gene_is_transcription_factor = hgnc_client.is_transcription_factor(gene)
            mapping[gene] = gene_is_transcription_factor
        return mapping
