import asyncio
import logging
import os
from typing import Union, Any

import aiohttp
import pandas as pd

logging.basicConfig(
    level=os.environ.get("LOGLEVEL", logging.INFO),
    format="%(asctime)s %(name)s - [%(levelname)s] > %(message)s",
)
logger = logging.getLogger("arweave-tracker")


async def get(session: aiohttp.ClientSession, url: str, timeout: int = 10):
    async with session.get(url, raise_for_status=True, timeout=timeout) as resp:
        return await resp.json()


async def batch_get(
        urls: list[str], timeout: int = 10, return_exceptions=False,
) -> tuple[
    Union[BaseException, Any], ...
]:
    async with aiohttp.ClientSession() as session:
        tasks = []
        for url in urls:
            tasks.append(asyncio.ensure_future(get(session, url, timeout=timeout)))
        results = await asyncio.gather(*tasks, return_exceptions=return_exceptions)
        return results


def append_csv(path: str, dicts: list[dict]):
    pd.DataFrame(dicts).to_csv(
        path,
        mode="a",
        index=False,
        header=not os.path.exists(path),
    )
