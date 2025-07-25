import logging
import struct

from .body import decode_body, encode_body
from .header import decode_header, encode_header


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
