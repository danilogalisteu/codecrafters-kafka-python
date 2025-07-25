import logging

from app.kafka import parse_message
from lib import curio

logging.basicConfig(
    format="[%(asctime)s|%(levelname)s] %(message)s", level=logging.INFO
)


async def client_cb(client, addr):
    logging.info(f"[{addr}] New connection")

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
                parsed_length, send_message = await parse_message(recv_message)

        recv_message = recv_message[parsed_length:]
        logging.warning(
            "[%s] Sending (%d) %s", addr, len(send_message), send_message.hex(" ")
        )
        await client.sendall(send_message)

    logging.info(f"[{addr}] Closing connection")


def main():
    print("Serving on localhost:9092...")
    curio.run(curio.tcp_server, "localhost", 9092, client_cb)


if __name__ == "__main__":
    main()
