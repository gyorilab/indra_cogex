Enrichment and Set Analysis
===========================

INDRA CoGEx has four different types of enrichment and set analysis tools accessible
at `discovery.indra.bio <http://discovery.indra.bio>`_ from the top panel or in the
first row of page cards under '*App and Services using INDRA CoGEx*'.

Discrete GSEA
-------------
This application performs gene set enrichment analysis given a list of
genes. The application ranks and lists GO terms, Reactome pathways, WikiPathway
pathways, as well as upstream or downstream biological entities (e.g., genes/proteins,
small molecules, biological processes) with respect to the input genes. Ranking is
based on the overlap between the input genes and the gene sets corresponding to each
listed term (e.g., the genes annotated with a given GO term or the genes that are part
of a given Reactome pathway). p-values are calculated using Fisher's exact test with
different options available to adjust for multiple hypothesis testing.

Form Fields
~~~~~~~~~~~

- **Include INDRA path-based analysis**: If checked, the application will also
  perform a path-based analysis using INDRA to identify upstream and downstream
  biological entities.
- **Minimum Evidence Count**: The minimum number of evidences to allow in INDRA
  statements supporting the path-based analysis. (Only available if the
  "Include INDRA path-based analysis" option is checked.)
- **Minumum Belief**: The minimum belief score to allow in INDRA statements
  supporting the path-based analysis. (Only available if the "Include INDRA
  path-based analysis" option is checked.)
- **Keep Insignificant Results**: If checked, the application will include
  results with insignificant p-values in the output.
- **Alpha**: The significance level for the Fisher's exact test with which multiple
  hypothesis testing correction will be executed.
- **Multiple Hypothesis Testing Correction**: The method to use for multiple
  hypothesis testing correction. Options are:

  - Family-wise Correction with Benjamini/Hochberg
  - Bonferroni (One-step Correction)
  - Sidak (One-step Correction)
  - Holm-Sidak (step-down method using Sidak adjustments)
  - Holm (step-down method using Bonferroni adjustments)
  - Two step Benjamini/Hochberg procedure
  - Two step estimation method of Benjamini, Krieger, and Yekutieli
- **Genes**: A list of genes separated by newlines or commas. The gene list can
  contain gene names, HGNC symbols, or CURIEs.

Signed GSEA
-----------
This application performs signed gene set enrichment analysis using INDRA CoGEx and the
`Reverse Causal Reasoning algorithm <https://doi.org/10.1186/1471-2105-14-340>`_.
[#reverse_causal_reasoning]_

.. [#reverse_causal_reasoning] Catlett, N. L., *et al.* (2013):
   `Reverse causal reasoning: applying qualitative causal knowledge to the
   interpretation of high-throughput data <https://doi.org/10.1186/1471-2105-14-340>`_.
   BMC Bioinformatics, **14** (1), 340.

Form Fields
~~~~~~~~~~~

- **Minimum Evidence Count**: The minimum number of evidences to allow in INDRA
  statements supporting the path-based analysis.
- **Minumum Belief**: The minimum belief score to allow in INDRA statements
  supporting the path-based analysis.
- **Keep Insignificant Results**: If checked, the application will include
  results with insignificant p-values in the output.
- **Alpha**: The significance level for the Fisher's exact test with which multiple
  hypothesis testing correction will be executed.
- **Positive Genes**: A list of genes separated by newlines or commas. The gene list
  can contain gene names, HGNC symbols, or CURIEs.
- **Negative Genes**: A list of genes separated by newlines or commas. The gene list
  can contain gene names, HGNC symbols, or CURIEs.

Continuous GSEA
---------------
This application performs GSEA using INDRA CoGEx on continuous data that is pre-ranked
by log2 fold change or another metric. GSEA is performed on the ranked list of genes by
`GSEAPreranked <https://www.gsea-msigdb.org/gsea/doc/GSEAUserGuideFrame.html?_GSEAPreranked_Page>`_
from the `Gene Set Enrichment Analysis (GSEA) software package <https://www.gsea-msigdb
.org/gsea/index.jsp>`_ developed by UC San Diego and the Broad Insitute using its Python
implementation in the `gseapy <http://gseapy.rtfd.io/>`_ package. [#gseapy_python]_

.. [#gseapy_python] Zhuoqing Fang, Xinyuan Liu, Gary Peltz, GSEApy: a comprehensive
   package for performing gene set enrichment analysis in Python, Bioinformatics, 2022;
   , btac757, `<https://doi.org/10.1093/bioinformatics/btac757>`_.

Form Fields
~~~~~~~~~~~

- **Minimum Evidence Count**: The minimum number of evidences to allow in INDRA
  statements supporting the path-based analysis, if using INDRA path-based analysis.
- **Minumum Belief**: The minimum belief score to allow in INDRA statements
  supporting the path-based analysis, if using INDRA path-based analysis.
- **Keep Insignificant Results**: If checked, the application will include
  results with insignificant p-values in the output.
- **Alpha**: The significance level for the Fisher's exact test with which multiple
  hypothesis testing correction will be executed.
- **Gene Set Source**: The source of the gene sets to use for the analysis. Options
  are:

  - GO
  - Reactome
  - WikiPathways
  - HPO Phenotypes
  - INDRA Upstream
  - INDRA Downstream
- **File**: The file containing the ranked gene list. The file should contain two
  columns of comma or tab-separated values: one with gene names (HGNC symbols) and one
  with the ranking metric values, e.g. log2 fold change. The first row should contain the
  column names provided in the "Gene Name Column" and "Ranking Metric Column" fields
  below.
- **Species**: The species of the gene set to use for the analysis. Options are:

  - Human
  - Rat
  - Mouse
- **Permutations**: The number of permutations to use for the GSEA analysis. Read more
  about the significance of the number of permutations in the `GSEA User Guide
  <https://www.gsea-msigdb.org/gsea/doc/GSEAUserGuideFrame.html?_GSEAPreranked_Page>`_.
- **Gene Name Column**: The name of the column in the file containing the gene names.
- **Ranking Metric Column**: The name of the column in the file containing the ranking
  metric values.

Discrete MSEA
-------------
This application performs metabolite set enrichment analysis using INDRA CoGEx,
p-values are calculated using Fisher's exact test with different options available to
adjust for multiple hypothesis testing.

Form Fields
~~~~~~~~~~~

- **Minimum Evidence Count**: The minimum number of evidences to allow in INDRA
  statements supporting the path-based analysis.
- **Minumum Belief**: The minimum belief score to allow in INDRA statements
  supporting the path-based analysis.
- **Keep Insignificant Results**: If checked, the application will include
  results with insignificant p-values in the output.
- **Alpha**: The significance level for the Fisher's exact test with which multiple
  hypothesis testing correction will be executed.
- **Multiple Hypothesis Testing Correction**: The method to use for multiple
  hypothesis testing correction. Options are:

  - Family-wise Correction with Benjamini/Hochberg
  - Bonferroni (One-step Correction)
  - Sidak (One-step Correction)
  - Holm-Sidak (step-down method using Sidak adjustments)
  - Holm (step-down method using Bonferroni adjustments)
  - Two step Benjamini/Hochberg procedure
  - Two step estimation method of Benjamini, Krieger, and Yekutieli
- **Metabolites**: A list of metabolites separated by newlines or commas. The
  metabolite list can contain CHEBI identifiers, or CURIEs.
