import requests
from bs4 import BeautifulSoup
import json
from tqdm import tqdm
import biomappings
import pyobo
from ratelimit import rate_limited

BASE = "https://go.drugbank.com"


def main():
    mappings = biomappings.load_mappings_subset("vo", "drugbank")
    drugbank_ids = sorted(mappings.values())
    _get_and_dump(drugbank_ids)


def get_all():
    # This is the ideal situation, but right now has some rate limiting issues
    drugbank_ids = pyobo.get_ids("drugbank")
    _get_and_dump(drugbank_ids)


def _get_and_dump(drugbank_ids):
    trials_info = {
        db: get_trials_from_api(db)
        for db in tqdm(sorted(drugbank_ids), unit="drugbank")
    }
    with open("drugbank_trials.json", "w") as file:
        json.dump(trials_info, file, indent=2)


def get_trials_from_api(drugbank_id: str):
    start = 0
    length = 50
    res = get_aggregate(drugbank_id, start, length)
    try:
        res_json = res.json()
    except requests.exceptions.JSONDecodeError as e:
        tqdm.write(f"[{res.status_code}] failed on {drugbank_id}\n\n{e}")
        return None

    rv = res_json["data"]

    while res_json["recordsTotal"] > start + length:
        start += length
        res = get_aggregate(drugbank_id, start, length)
        try:
            res_json = res.json()
        except requests.exceptions.JSONDecodeError:
            tqdm.write(
                f"[{res.status_code}] failed on {drugbank_id} - {start=}, {length}"
            )
            return None
        rv.extend(res_json["data"])

    rv = [_process_row(*row) for row in rv]
    return rv


@rate_limited
def get_aggregate(drugbank_id: str, length: int, start: int) -> requests.Response:
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
        max_phase=phases and max(phases),
        status=status,
        purpose=purpose,
        conditions=conditions,
        count=count_link.text,
        count_link=BASE + count_link.attrs["href"],
    )


if __name__ == "__main__":
    main()
