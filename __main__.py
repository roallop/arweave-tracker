from typing import Union

import fire
from util import logger
from arweave import ArweaveFetcher


class Tracker(object):
    def __init__(self, tags: list[dict[str, Union[str, list[str]]]]):
        self.fetcher = ArweaveFetcher(tags=tags)

    def start_tracking(self):
        logger.debug("Starting tracking")
        txs = self.fetcher.fetch_transactions(None, None, 10)
        logger.debug(f"Fetched {len(txs)} transactions: {txs}")


if __name__ == "__main__":
    tracker = Tracker(tags=[{"name": "App-Name", "values": ["MirrorXYZ"]}])
    fire.Fire(tracker)
