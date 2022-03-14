FROM python:3.10

RUN python -m pip install --upgrade pip
RUN python -m pip install --upgrade wheel
RUN python -m pip install gunicorn
RUN python -m pip install psycopg2-binary
RUN python -m pip install git+https://github.com/kkaris/indra_cogex.git@dockerize-frontend#egg=indra_cogex[web]
ENTRYPOINT python -m indra_cogex.apps.cli --port 5000 --host "0.0.0.0" --with-gunicorn --workers 2
