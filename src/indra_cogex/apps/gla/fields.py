from wtforms import (
    BooleanField,
    FileField,
    FloatField,
    IntegerField,
    RadioField,
    TextAreaField,
)
from wtforms.validators import DataRequired

genes_field = TextAreaField(
    "Genes",
    description="Paste your list of gene symbols, HGNC gene identifiers, or"
    ' CURIEs here or click here to use <a href="#" onClick="exampleGenes()">an'
    " example list of human genes</a> related to COVID-19.",
    validators=[DataRequired()],
)
positive_genes_field = TextAreaField(
    "Positive Genes",
    description="Paste your list of gene symbols, HGNC gene identifiers, or CURIEs here",
    validators=[DataRequired()],
)
negative_genes_field = TextAreaField(
    "Negative Genes",
    description="Paste your list of gene symbols, HGNC gene identifiers, or"
    ' CURIEs here or click here to use <a href="#" onClick="exampleGenes()">an'
    " example list</a> related to prostate cancer.",
    validators=[DataRequired()],
)
indra_path_analysis_field = BooleanField("Include INDRA path-based analysis (slow)")
minimum_evidence_field = IntegerField(
    "Minimum Evidence Count",
    default=1,
    description="The minimum number of evidences, if using INDRA path-based analysis.",
)
minimum_belief_field = FloatField(
    "Minimum Belief",
    default=0.0,
    description="The minimum belief score, if using INDRA path-based analysis.",
)
keep_insignificant_field = BooleanField(
    "Keep insignificant results (leads to long results lists)"
)
alpha_field = FloatField(
    "Alpha",
    default=0.05,
    validators=[DataRequired()],
    description="The alpha is the threshold for significance in the"
    " Fisher's exact test with which multiple hypothesis"
    " testing correction will be executed.",
)
correction_field = RadioField(
    "Multiple Hypothesis Test Correction",
    choices=[
        ("fdr_bh", "Family-wise Correction with Benjamini/Hochberg"),
        ("bonferroni", "Bonferroni (one-step correction)"),
        ("sidak", "Sidak (one-step correction)"),
        ("holm-sidak", "Holm-Sidak (step down method using Sidak adjustments)"),
        ("holm", "Holm (step-down method using Bonferroni adjustments)"),
        ("fdr_tsbh", "Two step Benjamini and Hochberg procedure"),
        (
            "fdr_tsbky",
            "Two step estimation method of Benjamini, Krieger, and Yekutieli",
        ),
    ],
    default="fdr_bh",
)
source_field = RadioField(
    "Gene Set Source",
    choices=[
        ("go", "Gene Ontology"),
        ("reactome", "Reactome"),
        ("wikipathways", "WikiPathways"),
        ("indra-upstream", "INDRA Upstream"),
        ("indra-downstrea", "INDRA Downstream"),
    ],
    default="go",
    description="The source of gene sets. Only one can be run at a time because"
    " this analyis is computationally intensive",
)
file_field = FileField("File", validators=[DataRequired()])
species_field = RadioField(
    "Species",
    choices=[
        ("human", "Human"),
        ("rat", "Rat"),
        ("mouse", "Mouse"),
    ],
    default="human",
)
permutations_field = IntegerField(
    "Permutations",
    default=100,
    validators=[DataRequired()],
    description="The number of permutations used with GSEA",
)
