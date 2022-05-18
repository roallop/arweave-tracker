import asyncio
import json
import os
import time
from typing import Union

from util import logger
from arweave import ArweaveFetcher


class Tracker(object):
    transactions_path = "transactions.jsonl"
    posts_path = "posts.jsonl"
    history_folder = "history"
    metrics_path = os.path.join(history_folder, "metrics.json")

    def __init__(self, tags: list[dict[str, Union[str, list[str]]]], transformer):
        self.fetcher = ArweaveFetcher(tags=tags, tags_transformer=transformer)
        os.makedirs(self.history_folder, exist_ok=True)

    def start_tracking(
        self,
        min_block: int = None,
        batch_size: int = 100,
        keep_tracking: bool = False,
        keep_recent_count: int = None,
        generate_feed: bool = True,
    ):
        start_time = time.time()
        logger.info(
            f"Starting tracking from block {min_block}, limit: {batch_size}, keep_tracking: {keep_tracking}"
        )
        while self._run_once(min_block, batch_size):
            if not keep_tracking:
                break
            if time.time() - start_time >= 1200:
                # commit every 20 min
                break

        if keep_recent_count:
            self.truncate(line_count=keep_recent_count)

        if generate_feed:
            self.generate_feed()

        # allow metrics fail
        try:
            self.generate_metric()
        except Exception as e:
            logger.error(f"Failed to generate metric: {e}")

    def _run_once(self, min_block: int, limit: int):
        txs, has_next, cursor = self.fetcher.fetch_transactions(
            cursor=self.fetcher.last_cursor, min_block=min_block, limit=limit
        )

        logger.info(
            f"Fetched {len(txs)} transactions, has_next: {has_next}, cursor: {cursor}"
        )
        if len(txs) == 0:
            return False

        ids = [tx["id"] for tx in txs]
        posts = asyncio.run(self.fetcher.batch_fetch_data(ids))
        logger.debug(f"Fetched {len(posts)} posts")

        # save after success
        self.fetcher.last_cursor = cursor
        self.append_to_file(self.transactions_path, txs)
        self.append_to_file(self.posts_path, posts)

        return has_next

    def truncate(self, interval: int = None, line_count: int = None):
        self._truncate(self.transactions_path, "block_timestamp", interval, line_count)
        self._truncate(self.posts_path, "timestamp", interval, line_count)

    @staticmethod
    def _truncate(path: str, timestamp_key: str, interval: int, line_count: int):
        if interval is None and line_count is None:
            return

        logger.info(
            f"Truncating {path} with interval: {interval}, line_count: {line_count}"
        )

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

    def generate_feed(self):
        from feed import generate_feed, feed_filename

        with open(self.posts_path, "r") as f:
            posts = [json.loads(line) for line in f.readlines()]
            posts = filter(lambda p: "error" not in p, posts)
            feed = generate_feed(posts)

        with open(feed_filename, "w") as f:
            feed.write(f, "utf-8")

    # json lines
    # append to current files and history files
    def append_to_file(self, path: str, dicts: list[dict]):
        with open(path, "a") as f, open(
            os.path.join(self.history_folder, path), "a"
        ) as hf:
            for d in dicts:
                s = json.dumps(d, ensure_ascii=False)
                f.write(s + "\n")
                hf.write(s + "\n")

    # TODO: history metric per day
    def generate_metric(self):
        with open(os.path.join(self.history_folder, self.posts_path), "r") as f:
            all_posts = [json.loads(line) for line in f.readlines()]
        logger.info(f"Generating metric from {len(all_posts)} history posts")
        if len(all_posts) == 0:
            return

        import pandas as pd

        time_time = time.time()
        one_day = time_time - 24 * 3600
        posts_24h = [p for p in all_posts if int(p["timestamp"]) > one_day]
        logger.info(f"Generating 24h metric from {len(posts_24h)} history posts")
        if len(posts_24h) == 0:
            return

        df = pd.DataFrame(posts_24h)
        post_count = len(df)
        user_count = df["contributor"].nunique()
        title_count = df["title"].nunique()
        body_count = df["body"].nunique()

        metrics = {
            "day1": {
                "post": post_count,
                "user": user_count,
                "title": title_count,
                "body": body_count,
            }
        }

        with open(self.metrics_path, "w") as f:
            f.write(json.dumps(metrics, ensure_ascii=False))
