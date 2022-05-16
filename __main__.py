from typing import Union

import fire
from util import logger
from arweave import ArweaveFetcher


# remove useless tags and flatten
def transform_tags(tags: list[dict]) -> dict:
    result = {}
    for item in tags:
        if item["name"] in {"App-Name", "Content-Type", "content-digest"}:
            continue
        result[item["name"].lower()] = item["value"]
    return result


class Tracker(object):
    def __init__(self, tags: list[dict[str, Union[str, list[str]]]], transformer):
        self.fetcher = ArweaveFetcher(tags=tags, tags_transformer=transformer)

    def start_tracking(self):
        logger.debug("Starting tracking")
        txs = self.fetcher.fetch_transactions(None, None, 10)
        logger.debug(f"Fetched {len(txs)} transactions: {txs}")


if __name__ == "__main__":
    tracker = Tracker(
        tags=[{"name": "App-Name", "values": ["MirrorXYZ"]}], transformer=transform_tags
    )
    fire.Fire(tracker)
