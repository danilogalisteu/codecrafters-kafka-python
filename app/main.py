import logging
import struct

import curio

logging.basicConfig(
    format="[%(asctime)s|%(levelname)s] %(message)s", level=logging.INFO
)


async def client_cb(client, addr):
    logging.info(f"[{addr}] New connection")

    recv_message = b""
    header_format = ">ihhi"
    header_size = struct.calcsize(header_format)
    while len(recv_message) < header_size:
        await curio.sleep(0)
        recv_message += await client.recv(100)
        logging.warning("[%s] Received %d %s", addr, len(recv_message), recv_message)

    req_size, req_api_key, req_api_version, correlation_id = struct.unpack(
        header_format, recv_message[:header_size]
    )
    logging.warning(
        "Header: %d %d %d %d", req_size, correlation_id, req_api_key, req_api_version
    )
    recv_message = recv_message[header_size:]

    error_code = 35 if req_api_version != 4 else 0
    throttle_time = 0

    send_message = b""
    send_message += struct.pack(">i", correlation_id)
    send_message += struct.pack(
        ">hBhhhb", error_code, 2, req_api_key, req_api_version, req_api_version, 0
    )
    send_message += struct.pack(">ib", throttle_time, 0)
    send_message = struct.pack(">i", len(send_message)) + send_message

    logging.warning("[%s] Sending %d %s", addr, len(send_message), send_message)
    await client.sendall(send_message)

    logging.info(f"[{addr}] Closing connection")


def main():
    logging.info("Serving...")
    curio.run(curio.tcp_server, "localhost", 9092, client_cb)


if __name__ == "__main__":
    main()
