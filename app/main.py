import logging
from pathlib import Path

from app.kafka import parse_message, read_meta
from lib import curio

logging.basicConfig(
    format="[%(asctime)s|%(levelname)s] %(message)s", level=logging.WARNING
)


async def client_cb(
    client: curio.io.Socket,
    addr: tuple[str, int],
    batches: list[
        dict[
            str,
            int
            | list[dict[str, str | int | bytes | list[int] | list[bytes] | list[str]]],
        ]
    ],
) -> None:
    logging.info("[%s] New connection", addr)

    recv_message = b""
    while True:
        parsed_length = 0
        while parsed_length == 0:
            await curio.sleep(0)
            recv_message += await client.recv(100)
            if len(recv_message) > 0:
                logging.warning(
                    "[%s] Received (%d) %s",
                    addr,
                    len(recv_message),
                    recv_message.hex(" "),
                )
                parsed_length, send_message = await parse_message(recv_message, batches)

        recv_message = recv_message[parsed_length:]
        logging.warning(
            "[%s] Sending (%d) %s", addr, len(send_message), send_message.hex(" ")
        )
        await client.sendall(send_message)

    logging.info("[%s] Closing connection", addr)


async def init() -> None:
    batches = await read_meta(
        Path("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log")
    )

    async def client_meta_cb(client: curio.io.Socket, addr: tuple[str, int]) -> None:
        return await client_cb(client, addr, batches)

    print("Serving on localhost:9092...")
    await curio.tcp_server("localhost", 9092, client_meta_cb)


def main() -> None:
    curio.run(init)


if __name__ == "__main__":
    main()
