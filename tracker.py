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

    def start_tracking(self):
        txs, has_next, cursor = self.fetcher.fetch_transactions(
            cursor=self.fetcher.last_cursor, min_block=None, limit=10
        )

        logger.debug(f"Fetched {len(txs)} transactions")
        ids = [tx["id"] for tx in txs]
        results = asyncio.run(self.fetcher.batch_fetch_data(ids))
        logger.debug(f"Fetched {len(results)} posts: {results}")

        # save after success
        self.fetcher.last_cursor = cursor
        self._save_transactions(txs)
        self._save_posts_results(ids, results)

    def _save_transactions(self, txs: list[dict]):
        append_to_file(self.transactions_path, txs)

    def _save_posts_results(
        self, ids: [str], posts: list[Union[dict, aiohttp.ClientResponseError]]
    ):
        final_posts = []
        for _id, post in zip(ids, posts):
            if isinstance(post, dict):
                final_posts.append(post)
            else:
                logger.warn(f"Error fetching post {_id}: {post}")
                final_posts.append({"id": _id, "error": post})
        append_to_file(self.posts_path, final_posts)
