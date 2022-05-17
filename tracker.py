import asyncio
import json
import time
from typing import Union

from util import logger, append_to_file
from arweave import ArweaveFetcher


class Tracker(object):
    transactions_path = "transactions.jsonl"
    posts_path = "posts.jsonl"

    def __init__(self, tags: list[dict[str, Union[str, list[str]]]], transformer):
        self.fetcher = ArweaveFetcher(tags=tags, tags_transformer=transformer)

    def start_tracking(
            self, min_block: int = None, batch_size: int = 100, keep_tracking: bool = False, keep_recent_count: int = None
    ):
        logger.info(
            f"Starting tracking from block {min_block}, limit: {batch_size}, keep_tracking: {keep_tracking}"
        )
        while self._run_once(min_block, batch_size):
            if not keep_tracking:
                break

        if keep_recent_count:
            self.truncate(line_count=keep_recent_count)

    def _run_once(self, min_block: int, limit: int):
        txs, has_next, cursor = self.fetcher.fetch_transactions(
            cursor=self.fetcher.last_cursor, min_block=min_block, limit=limit
        )

        logger.info(f"Fetched {len(txs)} transactions, has_next: {has_next}, cursor: {cursor}")
        if len(txs) == 0:
            return False

        ids = [tx["id"] for tx in txs]
        posts = asyncio.run(self.fetcher.batch_fetch_data(ids))
        logger.debug(f"Fetched {len(posts)} posts")

        # save after success
        self.fetcher.last_cursor = cursor
        append_to_file(self.transactions_path, txs)
        append_to_file(self.posts_path, posts)

        return has_next

    def truncate(self, interval: int = None, line_count: int = None):
        self._truncate(self.transactions_path, "block_timestamp", interval, line_count)
        self._truncate(self.posts_path, "timestamp", interval, line_count)

    @staticmethod
    def _truncate(path: str, timestamp_key: str, interval: int, line_count: int):
        if interval is None and line_count is None:
            return

        logger.info(f"Truncating {path} with interval: {interval}, line_count: {line_count}")

        with open(path, "r") as f:
            lines = f.readlines()

        if line_count is not None:
            if len(lines) <= line_count and interval is None:
                return
            lines = lines[-line_count:]

        # NOTE: truncate by time will cause transactions not match posts since they have different timestamp
        start_time = time.time() - interval if interval else None
        with open(path, "w") as f:
            for line in lines:
                if start_time is not None:
                    obj = json.loads(line)
                    if obj[timestamp_key] < start_time:
                        continue
                    # post is not ordered in same block, so we simply check every post
                    # start_time = None
                f.write(line)
