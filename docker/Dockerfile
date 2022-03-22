FROM python:3.10

RUN python -m pip install --upgrade pip && \
    python -m pip install --upgrade wheel && \
    python -m pip install git+https://github.com/bgyori/indra_cogex.git#egg=indra_cogex[web,gunicorn]

ENTRYPOINT python -m indra_cogex.apps.cli --port 5000 --host "0.0.0.0" --with-gunicorn --workers 2 --timeout 120
