FROM labsyspharm/indra

ARG INDRA_NEO4J_URL
ARG INDRA_NEO4J_USER
ARG INDRA_NEO4J_PASSWORD
ARG MGI_VERSION
ARG RGD_VERSION

RUN pip3 install git+https://github.com/bgyori/indra_cogex.git#egg=indra_cogex[web,gunicorn,gsea] && \
    python -m indra.ontology.bio build

# Copy the names.tsv files for the pre-built pyobo caches to this directory as
# mg_names.tsv and rgd_names.tsv, respectively. This is necessary to avoid running the
# full cache build process for these resources (which can take hours).
RUN mkdir -p /root/.data/pyobo/raw/mgi/${MGI_VERSION}/cache/ && \
    mkdir -p /root/.data/pyobo/raw/rgd/${RGD_VERSION}/cache/
COPY mgi_names.tsv /root/.data/pyobo/raw/mgi/${MGI_VERSION}/cache/names.tsv
COPY rgd_names.tsv /root/.data/pyobo/raw/rgd/${RGD_VERSION}/cache/names.tsv

# Force rebuild the pickle caches for the GSEA and MSEA apps
RUN python -m indra_cogex.client.enrichment.utils --force

ENTRYPOINT python -m indra_cogex.apps.cli --port 5000 --host "0.0.0.0" --with-gunicorn --workers 4 --timeout 300
