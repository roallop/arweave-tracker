import os
from typing import Union
import pandas as pd

from util import logger
from arweave import ArweaveFetcher


class Tracker(object):
    transactions_path = "transactions.csv"

    def __init__(self, tags: list[dict[str, Union[str, list[str]]]], transformer):
        self.fetcher = ArweaveFetcher(tags=tags, tags_transformer=transformer)

    def start_tracking(self):
        txs, has_next, cursor = self.fetcher.fetch_transactions(
            cursor=self.fetcher.last_cursor, min_block=None, limit=10
        )
        self.fetcher.last_cursor = cursor
        logger.debug(f"Fetched {len(txs)} transactions")
        self.save_transactions(txs)

    def save_transactions(self, txs: list[dict]):
        pd.DataFrame(txs).to_csv(
            self.transactions_path,
            mode="a",
            index=False,
            header=not os.path.exists(self.transactions_path),
        )
