import asyncio
import os
from typing import Union

import aiohttp
import pandas as pd

from util import logger, append_to_file
from arweave import ArweaveFetcher


class Tracker(object):
    transactions_path = "transactions.jsonl"
    posts_path = "posts.jsonl"

    def __init__(self, tags: list[dict[str, Union[str, list[str]]]], transformer):
        self.fetcher = ArweaveFetcher(tags=tags, tags_transformer=transformer)

    def start_tracking(
        self, min_block: int = None, batch_size: int = 100, keep_tracking: bool = False
    ):
        logger.info(
            f"Starting tracking from block {min_block}, limit: {batch_size}, keep_tracking: {keep_tracking}"
        )
        while self._run_once(min_block, batch_size):
            if not keep_tracking:
                break

    def _run_once(self, min_block: int, limit: int):
        txs, has_next, cursor = self.fetcher.fetch_transactions(
            cursor=self.fetcher.last_cursor, min_block=min_block, limit=limit
        )

        logger.debug(f"Fetched {len(txs)} transactions")
        ids = [tx["id"] for tx in txs]
        posts = asyncio.run(self.fetcher.batch_fetch_data(ids))
        logger.debug(f"Fetched {len(posts)} posts")

        # save after success
        self.fetcher.last_cursor = cursor
        append_to_file(self.transactions_path, txs)
        append_to_file(self.posts_path, posts)

        return has_next
