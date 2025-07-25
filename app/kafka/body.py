import struct

from .string import decode_string_compact


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
