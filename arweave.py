import json
import pickle
import sqlite3
from typing import Optional, List, Union

import pandas as pd
import requests
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport

from util import logger


class ArweaveFetcher(object):
    # tags are graphql str
    # e.g. "[{ name: "App-Name", values: ["MirrorXYZ"] }]"
    def __init__(
        self,
        tags: list[dict[str, Union[str, list[str]]]],
        url="https://arweave.net",
        timeout=30,
    ):
        transport = AIOHTTPTransport(url=url + "/graphql", timeout=timeout)
        self.client = Client(transport=transport, execute_timeout=timeout)
        self.url = url
        self.timeout = timeout
        self.tags = tags

    def execute(self, query: str, variables: dict = None) -> dict:
        result = self.client.execute(gql(query), variable_values=variables)
        return result

    def current_block_height(self) -> int:
        return self.execute(
            """
query {
  blocks(first: 1, sort: HEIGHT_DESC) {
    edges {
      node {
        height
      }
    }
  }
}
        """
        )["blocks"]["edges"][0]["node"]["height"]

    @property
    def last_cursor(self) -> Optional[str]:
        try:
            with open(".last_cursor", "rb") as f:
                return pickle.load(f)
        except FileNotFoundError:
            return None

    @last_cursor.setter
    def last_cursor(self, value: str):
        with open(".last_cursor", "wb") as f:
            pickle.dump(value, f)

    def fetch_transactions(
        self, cursor: Optional[str], min_block: Optional[int], limit=100
    ) -> tuple[List[dict], bool, Optional[str]]:
        if min_block is None:
            min_block = 934740
        logger.debug(f"start with cursor: {cursor}, min_block: {min_block}")
        result = self.execute(
            """
query($cursor: String, $min_block: Int, $tags: [TagFilter!]!, $limit: Int!) {
  transactions(
    tags: $tags
    sort: HEIGHT_ASC
    first: $limit
    after: $cursor
    block: { min: $min_block }
  ) {
    edges {
      cursor
      node {
        id
        tags {
          name
          value
        }
        block {
          height
          timestamp
        }
      }
    }
    pageInfo {
      hasNextPage
    }
  }
}
""",
            variables={
                "cursor": cursor,
                "limit": limit,
                "min_block": min_block,
                "tags": self.tags,
            },
        )
        has_next_page = result["transactions"]["pageInfo"]["hasNextPage"]

        edges = result["transactions"]["edges"]
        next_cursor = edges[-1]["cursor"] if len(edges) > 0 else None
        return [self.edge_to_transaction(e) for e in edges], has_next_page, next_cursor

    @staticmethod
    def edge_to_transaction(e: dict) -> dict:
        n = e["node"]
        result = {
            "id": n["id"],
            "tags": json.dumps(n["tags"]),
            "block_height": n["block"]["height"],
            "block_timestamp": n["block"]["timestamp"],
        }
        return result
