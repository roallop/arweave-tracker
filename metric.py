import glob
import json
import logging

import pandas as pd
import matplotlib.pyplot as plt

from util import logger


class Metric(object):
    def recent_history_objects(self, key: str, limit: int):
        results = []
        files = self.recent_history_files(key, limit)
        logger.info(f"loaded {len(files)} files: {files}")
        for path in files:
            objs = [json.loads(line) for line in open(path, "r").readlines()]
            results.extend([o for o in objs if "error" not in o])
        return results

    def recent_history_files(self, key: str, limit: int):
        return sorted(glob.glob(f"./history/{key}_**.jsonl"))[-limit:]


def post():
    m = Metric()

    objs = m.recent_history_objects("posts", 10)
    logger.info(f"loaded {len(objs)} objects: \n{objs[0]['timestamp']}\n{objs[-1]['timestamp']}")
    df = pd.DataFrame(objs)
    logger.info(f"loaded {len(df)}\n{df.head()}")
    # logger.info(f"loaded {len(df)} {df['timestamp'].to_string()}")
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
    df["post"] = df["id"]
    df = df[["datetime", "post", "contributor", "title", "body"]]
    df = df.groupby(pd.Grouper(key='datetime', freq='1d')).nunique()
    # df.set_index("datetime", inplace=True)
    print(df.to_string())
    df.plot(legend=True, figsize=(12, 12))
    # plt.savefig("posts.png")
    plt.show()


def txs():
    m = Metric()

    objs = m.recent_history_objects("transactions", 10)
    logger.info(f"loaded {len(objs)} objects: \n{objs[0]['block_timestamp']}\n{objs[-1]['block_timestamp']}")
    df = pd.DataFrame(objs)
    logger.info(f"loaded {len(df)}\n{df.head()}")
    # logger.info(f"loaded {len(df)} {df['timestamp'].to_string()}")
    df['datetime'] = pd.to_datetime(df['block_timestamp'], unit='s')
    df["post"] = df["id"]
    df = df[["datetime", "post"]]
    df = df.groupby(pd.Grouper(key='datetime', freq='1d')).nunique()
    # df.set_index("datetime", inplace=True)
    print(df.to_string())
    df.plot(legend=True, figsize=(12, 12))
    # plt.savefig("posts.png")
    plt.show()


def print_per_file():
    m = Metric()

    files = m.recent_history_files("transactions", 1000)
    for path in files:
        lines = open(path, "r").readlines()
        logger.info(f"{path.removeprefix('./history/transactions_').removesuffix('.jsonl')}: {len(lines)}")


if __name__ == "__main__":
    # post()
    txs()
    # print_per_file()
