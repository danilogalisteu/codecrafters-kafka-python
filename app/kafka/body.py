import struct

from .constants import ApiKey
from .types.string import decode_string_compact


def decode_body_apiversions(recv_message):
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


def encode_body_apiversions(error_code, throttle_time):
    send_message = struct.pack(">hB", error_code, 3)
    send_message += struct.pack(
        ">hhhB",
        ApiKey.ApiVersions,
        0,
        4,
        0,
    )
    send_message += struct.pack(
        ">hhhB",
        ApiKey.DescribeTopicPartitions,
        0,
        0,
        0,
    )
    send_message += struct.pack(">iB", throttle_time, 0)
    return send_message
