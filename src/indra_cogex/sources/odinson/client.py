import requests
from indra.config import get_config

ODINSON_URL = get_config("ODINSON_URL")


def send_request(grammar, url=ODINSON_URL):
    obj = {"grammar": grammar, "pageSize": 10, "allowTriggerOverlaps": False}
    endpoint = url + "/api/execute/grammar"
    resp = requests.post(endpoint, json=obj)
    resp.raise_for_status()
    return resp.json()
