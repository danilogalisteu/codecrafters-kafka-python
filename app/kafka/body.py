import struct

from .constants import ApiKey
from .types.string import decode_string_compact
from .types.varint import decode_varint, encode_varint


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


def decode_body_describetopicpartitions(recv_message):
    parsed_length, array_length = decode_varint(recv_message)
    if parsed_length == 0:
        return 0, 0, 0
    recv_message = recv_message[parsed_length:]

    topic_array = []
    for _ in range(1, array_length):
        parsed_topic, topic = decode_string_compact(recv_message)
        if parsed_topic == 0:
            return 0, 0, 0
        recv_message = recv_message[parsed_topic:]

        if len(recv_message) < 1:
            return 0, 0, 0
        assert recv_message[0] == 0
        recv_message = recv_message[1:]

        topic_array.append(topic)
        parsed_length += parsed_topic + 1

    if len(recv_message) < 5:
        return 0, 0, 0
    partition_limit, cursor, = struct.unpack(">IB", recv_message[:5])
    recv_message = recv_message[5:]

    if len(recv_message) < 1:
        return 0, 0, 0
    assert recv_message[0] == 0

    return parsed_length + 5 + 1, topic_array, partition_limit, cursor


def encode_body_apiversions(error_code, throttle_time):
    send_message = struct.pack(">h", error_code)
    send_message += encode_varint(3)
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
