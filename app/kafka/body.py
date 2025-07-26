import struct

from .constants import ApiKey, ErrorCode, TagBuffer
from .types.string import decode_string_compact, encode_string_compact
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
    assert recv_message[0] == TagBuffer

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
        assert recv_message[0] == TagBuffer
        recv_message = recv_message[1:]

        topic_array.append(topic)
        parsed_length += parsed_topic + 1

    if len(recv_message) < 5:
        return 0, 0, 0
    (
        partition_limit,
        cursor,
    ) = struct.unpack(">IB", recv_message[:5])
    recv_message = recv_message[5:]

    if len(recv_message) < 1:
        return 0, 0, 0
    assert recv_message[0] == TagBuffer

    return parsed_length + 5 + 1, topic_array, partition_limit, cursor


def encode_body_apiversions(api_version, throttle_time):
    error_code = ErrorCode.UNSUPPORTED_VERSION if api_version != 4 else ErrorCode.NONE
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
    send_message += struct.pack(">iB", throttle_time, TagBuffer)
    return send_message


def encode_body_describetopicpartitions(topic_array, throttle_time):
    send_message = struct.pack(">I", throttle_time)
    send_message += encode_varint(1 + len(topic_array))

    for topic in topic_array:
        error_code = ErrorCode.UNKNOWN_TOPIC_OR_PARTITION
        topic_id = (0).to_bytes(16)
        is_internal = (0).to_bytes(1)
        auth_operations = 0x00000DF8
        next_cursor = 0xFF
        send_message += struct.pack(">h", error_code)
        send_message += encode_string_compact(topic)
        send_message += topic_id
        send_message += is_internal
        send_message += encode_varint(1)  # empty partitions array
        send_message += struct.pack("<IB", auth_operations, TagBuffer)

    send_message += struct.pack("BB", next_cursor, TagBuffer)
    return send_message
