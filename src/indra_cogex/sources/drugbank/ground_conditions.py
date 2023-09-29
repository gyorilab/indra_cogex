import gilda
import json
from tqdm import tqdm


def main():
    with open("conditions.json") as file:
        data = json.load(file)

    ungrounded = set()
    for drugbank_condition_id, name in tqdm(
        data.items(), unit_scale=True, unit="condition"
    ):
        g = gilda.ground(name)
        if not g:
            ungrounded.add((drugbank_condition_id, name))
    print(
        f"{len(ungrounded):,}/{len(data):,} ({len(ungrounded)/len(data):.1%}) could not be automatically grounded"
    )


if __name__ == "__main__":
    main()
