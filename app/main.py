import logging
import struct

import curio

logging.basicConfig(
    format="[%(asctime)s|%(levelname)s] %(message)s", level=logging.INFO
)


def decode_string_compact(recv_message):
    if len(recv_message) < 1:
        return 0, b""
    length = recv_message[0]
    if len(recv_message) < length:
        return 0, b""
    return length, recv_message[1:length].decode()


def decode_string(recv_message):
    if len(recv_message) < 2:
        return 0, b""
    (length,) = struct.unpack(">h", recv_message[:2])
    if len(recv_message) < 2 + length:
        return 0, b""
    return 2 + length, recv_message[2 : 2 + length].decode()


def decode_header(recv_message):
    header_format = ">hhi"
    header_size = struct.calcsize(header_format)
    if len(recv_message) < header_size:
        return 0, 0, 0, 0, 0
    api_key, api_version, correlation_id = struct.unpack(
        header_format, recv_message[:header_size]
    )
    recv_message = recv_message[header_size:]

    parsed, client_id = decode_string(recv_message)
    if parsed == 0:
        return 0, 0, 0, 0, 0
    recv_message = recv_message[parsed:]

    if len(recv_message) < 1:
        return 0, 0, 0, 0, 0
    assert recv_message[0] == 0, recv_message.hex(" ")

    return header_size + parsed + 1, api_key, api_version, correlation_id, client_id


def decode_body(recv_message):
    parsed_id, body_client_id = decode_string_compact(recv_message)
    if parsed_id == 0:
        return 0, 0, 0
    recv_message = recv_message[parsed_id:]

    parsed_sw, body_sw_version = decode_string_compact(recv_message)
    if parsed_sw == 0:
        return 0, 0, 0
    recv_message = recv_message[parsed_sw:]

    if len(recv_message) < 1:
        return 0, 0, 0
    assert recv_message[0] == 0

    return parsed_id + parsed_sw + 1, body_client_id, body_sw_version


def encode_header(correlation_id):
    return struct.pack(">i", correlation_id)


def encode_body(error_code, api_key, min_api_version, max_api_version, throttle_time):
    send_message = struct.pack(">h", error_code)
    send_message += struct.pack(
        ">BhhhB",
        2,
        api_key,
        min_api_version,
        max_api_version,
        0,
    )
    send_message += struct.pack(">iB", throttle_time, 0)
    return send_message


async def parse_message(recv_message):
    if len(recv_message) < 4:
        return 0, b""

    (message_size,) = struct.unpack(">i", recv_message[:4])

    parsed_header, api_key, api_version, correlation_id, client_id = decode_header(
        recv_message[4:]
    )
    if parsed_header == 0:
        return 0, b""

    logging.warning(
        "Header: (%d) %d %d %d %s",
        message_size,
        api_key,
        api_version,
        correlation_id,
        client_id,
    )

    parsed_body, body_client_id, body_sw_version = decode_body(
        recv_message[4 + parsed_header :]
    )
    if parsed_body == 0:
        return 0, b""

    assert message_size == parsed_header + parsed_body, "unexpected message size"

    logging.warning(
        "Body: %s %s",
        body_client_id,
        body_sw_version,
    )

    error_code = 35 if api_version != 4 else 0
    throttle_time = 0

    send_message = encode_header(correlation_id)
    send_message += encode_body(
        error_code, api_key, api_version, api_version, throttle_time
    )

    return 4 + message_size, struct.pack(">i", len(send_message)) + send_message


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
    logging.info("Serving...")
    curio.run(curio.tcp_server, "localhost", 9092, client_cb)


if __name__ == "__main__":
    main()
