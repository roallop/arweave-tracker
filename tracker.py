import asyncio
import os
from typing import Union

import aiohttp
import pandas as pd

from util import logger, append_csv
from arweave import ArweaveFetcher


class Tracker(object):
    transactions_path = "transactions.csv"
    posts_path = "posts.csv"

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
        pd.DataFrame(txs).to_csv(
            self.transactions_path,
            mode="a",
            index=False,
            header=not os.path.exists(self.transactions_path),
        )

    def _save_posts_results(
        self, ids: [str], posts: list[Union[dict, aiohttp.ClientResponseError]]
    ):
        final_posts = []
        for _id, post in zip(ids, posts):
            if isinstance(post, dict):
                final_posts.append(post)
            else:
                if post.status == 404:
                    final_posts.append({"id": _id, "error": "Not found"})
                else:
                    logger.warn(f"Error fetching post {_id}: {post}")
        append_csv(self.posts_path, final_posts)
