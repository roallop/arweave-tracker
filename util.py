import asyncio
import json
import logging
import os
from typing import Union, Any

import aiohttp

logging.basicConfig(
    level=os.environ.get("LOGLEVEL", logging.INFO),
    format="%(asctime)s %(name)s - [%(levelname)s] > %(message)s",
)
logger = logging.getLogger("arweave-tracker")


async def get(session: aiohttp.ClientSession, url: str, timeout: int = 10):
    async with session.get(url, raise_for_status=True, timeout=timeout) as resp:
        return await resp.json()


async def batch_get(
    urls: list[str],
    timeout: int = 10,
    return_exceptions=False,
) -> tuple[Union[BaseException, Any], ...]:
    async with aiohttp.ClientSession() as session:
        tasks = []
        for url in urls:
            tasks.append(asyncio.ensure_future(get(session, url, timeout=timeout)))
        results = await asyncio.gather(*tasks, return_exceptions=return_exceptions)
        return results


# json lines
def append_to_file(path: str, dicts: list[dict]):
    with open(path, "a") as f:
        for d in dicts:
            f.write(json.dumps(d, ensure_ascii=False) + "\n")
