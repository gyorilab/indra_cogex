FROM python:3.10

RUN python -m pip install --upgrade pip
RUN python -m pip install --upgrade wheel
RUN python -m pip install gunicorn
RUN python -m pip install git+https://github.com/kkaris/indra_cogex.git@dockerize-frontend[web]
ENTRYPOINT python -m  --port 8768 --host "0.0.0.0" --with-gunicorn --workers 2
