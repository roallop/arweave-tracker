import json
from typing import Optional, List, Union

import aiohttp
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport

from util import logger, batch_get


class ArweaveFetcher(object):
    # tags are graphql str
    # e.g. "[{ name: "App-Name", values: ["MirrorXYZ"] }]"
    def __init__(
        self,
        tags: list[dict[str, Union[str, list[str]]]],
        url="https://arweave.net",
        timeout=30,
        # mapping tags
        tags_transformer=None,
    ):
        transport = AIOHTTPTransport(url=url + "/graphql", timeout=timeout)
        self.client = Client(transport=transport, execute_timeout=timeout)
        self.url = url
        self.timeout = timeout
        self.tags = tags
        self.tags_transformer = tags_transformer

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
            with open(".last_cursor", "r") as f:
                return f.read().strip()
        except FileNotFoundError:
            return None

    @last_cursor.setter
    def last_cursor(self, value: str):
        with open(".last_cursor", "w") as f:
            f.write(value)

    def fetch_transactions(
        self, cursor: Optional[str], min_block: Optional[int], limit=100
    ) -> tuple[List[dict], bool, Optional[str]]:
        if min_block is None:
            min_block = 935000
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

    def edge_to_transaction(self, e: dict) -> dict:
        n = e["node"]
        result = {
            "id": n["id"],
            "block_height": n["block"]["height"],
            "block_timestamp": n["block"]["timestamp"],
        }
        if t := self.tags_transformer:
            converted = t(n["tags"])
            for k, v in converted.items():
                result[k] = v
        else:
            result["tags"] = json.dumps(n["tags"])
        return result

    async def batch_fetch_data(self, _ids: List[str]) -> [dict]:
        if len(_ids) == 0:
            return []

        urls = [self.url + "/" + _id for _id in _ids]

        def resp_post_to_db_post(_id: str, post) -> dict:
            if not isinstance(post, dict):
                logger.warn(f"[{_id}] error: {post}")
                if isinstance(post, aiohttp.ClientResponseError):
                    return {
                        "id": _id,
                        "error": {"status": post.status, "message": post.message},
                    }
                return {"id": _id, "error": {"message": f"unknown error: {post}"}}

            content = post["content"]
            dbpost = {
                "id": _id,
                "title": content["title"],
                "body": content["body"],
                "timestamp": int(content["timestamp"]),
                "digest": post["digest"],
                "contributor": post["authorship"]["contributor"],
            }
            if nft := post.get("nft"):
                if len(nft) > 0:
                    dbpost["nft"] = json.dumps(nft)
            return dbpost

        results = await batch_get(urls, timeout=self.timeout, return_exceptions=True)
        return [resp_post_to_db_post(_id, post) for _id, post in zip(_ids, results)]
