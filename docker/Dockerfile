FROM labsyspharm/indra

RUN pip3 install git+https://github.com/bgyori/indra_cogex.git#egg=indra_cogex[web,gunicorn,gsea]

ENTRYPOINT python -m indra_cogex.apps.cli --port 5000 --host "0.0.0.0" --with-gunicorn --workers 4 --timeout 300