import logging
from pathlib import Path

from lib import curio

from .batch import decode_batch


async def read_meta(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        return []

    buffer = b""
    async with curio.aopen(path, "rb") as f:
        buffer += await f.readall()

    if not buffer:
        return []

    batches = []
    while True:
        pos, batch = decode_batch(buffer)
        if pos == 0:
            break
        batches.append(batch)
        buffer = buffer[pos:]
        logging.info("New batch %d %s", pos, batch)

    if buffer:
        logging.warning("Remaining meta data %s", buffer.hex(" "))

    return batches
