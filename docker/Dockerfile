FROM ubuntu:22.04

ARG INDRA_NEO4J_URL
ARG INDRA_NEO4J_USER
ARG INDRA_NEO4J_PASSWORD
ARG MGI_VERSION
ARG RGD_VERSION

# Copy the names.tsv files for the pre-built pyobo caches to this directory as
# mg_names.tsv and rgd_names.tsv, respectively. This is necessary to avoid running the
# full cache build process for these resources (which can take hours).
RUN mkdir -p /root/.data/pyobo/raw/mgi/${MGI_VERSION}/cache/ && \
    mkdir -p /root/.data/pyobo/raw/rgd/${RGD_VERSION}/cache/
COPY mgi_names.tsv /root/.data/pyobo/raw/mgi/${MGI_VERSION}/cache/names.tsv
COPY rgd_names.tsv /root/.data/pyobo/raw/rgd/${RGD_VERSION}/cache/names.tsv

# Setup copied and slightly modified from indra_docker Dockerfile
ARG INDRA_BUILD_BRANCH

# Set working folder
ENV DIRPATH /sw
WORKDIR $DIRPATH

RUN apt-get update && \
    # Install Java
    apt-get install -y openjdk-8-jdk && \
    # jnius-indra requires cython which requires gcc
    apt-get install -y git wget zip unzip bzip2 gcc graphviz graphviz-dev \
        pkg-config python3-pip cmake libxml2-dev swig && \
    ln -s /usr/bin/python3 /usr/bin/python

# Set default character encoding
# See http://stackoverflow.com/questions/27931668/encoding-problems-when-running-an-app-in-docker-python-java-ruby-with-u/27931669
# See http://stackoverflow.com/questions/39760663/docker-ubuntu-bin-sh-1-locale-gen-not-found
RUN apt-get install -y locales && \
    locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

# Set environment variables
ENV BNGPATH $DIRPATH/BioNetGen-2.4.0
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64

RUN git clone https://github.com/sorgerlab/indra.git && \
    cd indra && \
    git checkout $INDRA_BUILD_BRANCH && \
    git branch && \
    mkdir /root/.config && \
    mkdir /root/.config/indra && \
    echo "[indra]" > /root/.config/indra/config.ini && \
    # Install Python dependencies
    pip install --upgrade pip && \
    # Install cython first for pyjnius
    pip install "cython<3" && \
    pip install "pyjnius==1.1.4" && \
    pip install --prefer-binary python-libsbml || true && \
    pip install -e .[all] && \
    pip uninstall -y enum34 && \
    # Pre-build the bio ontology
    python -m indra.ontology.bio build && \
    # Download Adeft models
    python -m adeft.download && \
    # Download protmapper resources
    python -m protmapper.resources && \
    # Download gilda resources
    python -m gilda.resources && \
    # Install BioNetGen
    cd $DIRPATH && \
    wget "https://github.com/RuleWorld/bionetgen/releases/download/BioNetGen-2.4.0/BioNetGen-2.4.0-Linux.tgz" \
        -O bionetgen.tar.gz -nv && \
    tar xzf bionetgen.tar.gz && \
    # Install things related to API deployment
    pip install gunicorn

# CoGEx specific setup
RUN pip3 install git+https://github.com/bgyori/indra_cogex.git#egg=indra_cogex[web,gunicorn,gsea]

# Force rebuild the pickle caches for the GSEA and MSEA apps
RUN python -m indra_cogex.client.enrichment.utils --force

# Save agent name in cache for search
RUN python -m indra_cogex.apps.search.utils --cache

ENV GUNICORN_CONF /usr/local/lib/python3.10/dist-packages/indra_cogex/apps/gunicorn.conf.py

ENTRYPOINT gunicorn -w 2 -c $GUNICORN_CONF -t 300 -b 0.0.0.0:5000 indra_cogex.apps.wsgi:app
