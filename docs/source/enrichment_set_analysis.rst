Enrichment and Set Analysis
===========================

INDRA CoGEx has four different types of enrichment and set analysis tools accessible
at `discovery.indra.bio <http://discovery.indra.bio>`_.

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

Signed GSEA
-----------
This application performs signed gene set enrichment analysis using INDRA CoGEx and the
Reverse Causal Reasoning algorithm.

**TODO: more details**

Continuous GSEA
---------------
This application performs GSEA on continuous data using INDRA CoGEx.

**TODO: more details**

Discrete MSEA
-------------
This application performs metabolite set enrichment analysis using INDRA CoGEx,
p-values are calculated using Fisher's exact test with different options available to
adjust for multiple hypothesis testing.

**TODO: more details**
