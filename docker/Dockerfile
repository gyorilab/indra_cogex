FROM labsyspharm/indra

ARG INDRA_NEO4J_URL
ARG INDRA_NEO4J_USER
ARG INDRA_NEO4J_PASSWORD
ARG MGI_VERSION
ARG RGD_VERSION

RUN pip3 install git+https://github.com/bgyori/indra_cogex.git#egg=indra_cogex[web,gunicorn,gsea] && \
    python -m indra.ontology.bio build

# Symlink the directories for the caches to this directory then run the docker build
# Copy the pre-built pyobo caches for mouse (mgi) and rat (rgd) for pyobo
COPY mgi/metadata.json /root/.data/pyobo/raw/mgi/
COPY mgi/${MGI_VERSION} /root/.data/pyobo/raw/mgi/${MGI_VERSION}
COPY rgd/metadata.json /root/.data/pyobo/raw/rgd/
COPY rgd/${RGD_VERSION} /root/.data/pyobo/raw/rgd/${RGD_VERSION}

RUN python -m indra_cogex.client.enrichment.utils --force

ENTRYPOINT python -m indra_cogex.apps.cli --port 5000 --host "0.0.0.0" --with-gunicorn --workers 4 --timeout 300
