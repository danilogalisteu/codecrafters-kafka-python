import logging
import struct

import curio

logging.basicConfig(
    format="[%(asctime)s|%(levelname)s] %(message)s", level=logging.INFO
)


async def client_cb(client, addr):
    logging.warning(f"[{addr}] New connection")


def main():
    logging.warning("Logs from your program will appear here!")

    curio.run(curio.tcp_server, "localhost", 9092, client_cb)


if __name__ == "__main__":
    main()
