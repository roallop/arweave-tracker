from typing import Union

from util import logger
from arweave import ArweaveFetcher


class Tracker(object):
    def __init__(self, tags: list[dict[str, Union[str, list[str]]]], transformer):
        self.fetcher = ArweaveFetcher(tags=tags, tags_transformer=transformer)

    def start_tracking(self):
        txs, has_next, cursor = self.fetcher.fetch_transactions(
            cursor=self.fetcher.last_cursor, min_block=None, limit=10
        )
        self.fetcher.last_cursor = cursor
        logger.debug(f"Fetched {len(txs)} transactions")
