from pathlib import Path

import requests
from bs4 import BeautifulSoup
import json
import time
from tqdm import tqdm
import biomappings
import pyobo
import pystow
import click
import gzip

BASE = "https://go.drugbank.com"
VERSION = "5.1.10"  # todo use bioversions/drugbank_downloader
HERE = Path(__file__).parent.resolve()


def main():
    mappings = biomappings.load_mappings_subset("vo", "drugbank")
    drugbank_ids = sorted(mappings.values())
    _get_and_dump(drugbank_ids, version=VERSION)


def get_all():
    # This is the ideal situation, but right now has some rate limiting issues
    drugbank_ids = pyobo.get_ids("drugbank")
    click.echo(f"got {len(drugbank_ids):,} drugbank ids")
    _get_and_dump(drugbank_ids, version=VERSION)


def _get_and_dump(drugbank_ids, *, version: str):
    trials_info = {}
    conditions = {}
    for drugbank_id in tqdm(
        sorted(drugbank_ids),
        unit="drug",
        unit_scale=True,
        desc="Getting DrugBank trials",
    ):
        trials_info[drugbank_id] = data = get_trials_from_api(
            drugbank_id, version=version
        )
        for trial in data:
            for condition in trial["conditions"]:
                conditions[condition["identifier"]] = condition["name"]

    with gzip.open(HERE.joinpath("drugbank_trials.json.gz"), "wt") as file:
        json.dump(trials_info, file, indent=2)

    with HERE.joinpath("drugbank_trials_sample.json").open("w") as file:
        json.dump({"DB01536": trials_info["DB01536"]}, file, indent=2)

    with HERE.joinpath("conditions.json").open("w") as file:
        json.dump(conditions, file, indent=2, sort_keys=True)


def get_trials_from_api(drugbank_id: str, *, version: str):
    cache_path = pystow.join("drugbank", version, "trials", name=f"{drugbank_id}.json")
    if cache_path.is_file():
        return json.loads(cache_path.read_text())
    start = 0
    length = 50
    res = get_aggregate(drugbank_id, start=start, length=length)
    try:
        res_json = res.json()
    except requests.exceptions.JSONDecodeError as e:
        tqdm.write(f"[{res.status_code}] failed on {drugbank_id}\n\n{e}")
        return None

    rv = res_json["data"]

    while res_json["recordsTotal"] > start + length:
        start += length
        res = get_aggregate(drugbank_id, start=start, length=length)
        try:
            res_json = res.json()
        except requests.exceptions.JSONDecodeError:
            tqdm.write(
                f"[{res.status_code}] failed on {drugbank_id} - {start=}, {length}"
            )
            return None
        rv.extend(res_json["data"])

    rv = [_process_row(*row) for row in rv]

    cache_path.write_text(json.dumps(rv, indent=2, ensure_ascii=False, sort_keys=True))
    return rv


def get_aggregate(drugbank_id: str, *, length: int, start: int) -> requests.Response:
    time.sleep(0.5)
    url = f"{BASE}/drugs/{drugbank_id}/clinical_trials/aggregate.json?length={length}&start={start}"
    tqdm.write(f"Requesting {url}")
    res = requests.get(url)
    return res


def _process_row(phases, status, purpose, conditions_raw, count_raw):
    condition_soup = BeautifulSoup(conditions_raw, features="html.parser")
    conditions = [
        {
            "name": link.text,
            "identifier": link.attrs["href"].removeprefix("/indications/"),
            # "link": BASE + link.attrs["href"],
        }
        for link in condition_soup.find_all("a")
    ]

    count_soup = BeautifulSoup(count_raw, features="html.parser")
    count_link = count_soup.find("a")

    if phases == "<span class='not-available'>Not Available</span>":
        phases = []
    else:
        phases = [x.strip() for x in phases.split(",")]
    if purpose == "<span class='not-available'>Not Available</span>":
        purpose = None
    return dict(
        phases=phases,
        max_phase=phases and max(phases),
        status=status,
        purpose=purpose,
        conditions=conditions,
        count=count_link.text,
        count_link=BASE + count_link.attrs["href"],
    )


if __name__ == "__main__":
    get_all()
