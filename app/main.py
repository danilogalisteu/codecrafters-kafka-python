import logging
import struct

import curio

logging.basicConfig(
    format="[%(asctime)s|%(levelname)s] %(message)s", level=logging.INFO
)


async def client_cb(client, addr):
    logging.warning(f"[{addr}] New connection")

    recv_message = b""
    header_size = struct.calcsize(">ii")
    while len(recv_message) < header_size:
        await curio.sleep(0)
        recv_message += await client.recv(100)
        logging.warning("[%s] Received %d %s", addr, len(recv_message), recv_message)

    message_size, correlation_id = struct.unpack(">ii", recv_message[:header_size])
    logging.warning("%d %d", message_size, correlation_id)
    recv_message = recv_message[header_size:]

    send_message = struct.pack(">ii", 0, 7)
    logging.warning("[%s] Sending %s", addr, send_message)
    await client.sendall(send_message)


def main():
    logging.warning("Logs from your program will appear here!")

    curio.run(curio.tcp_server, "localhost", 9092, client_cb)


if __name__ == "__main__":
    main()
