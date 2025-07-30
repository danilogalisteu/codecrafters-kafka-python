import struct

from app.kafka.constants import ApiKey, ErrorCode, TagBuffer
from app.kafka.types.string import decode_string_compact, encode_string_compact
from app.kafka.types.varint import decode_varint, encode_varint


def decode_body_apiversions(recv_message: bytes) -> tuple[int, str, str]:
    parsed_id, body_client_id = decode_string_compact(recv_message)
    if parsed_id == 0:
        return 0, "", ""
    recv_message = recv_message[parsed_id:]

    parsed_sw, body_sw_version = decode_string_compact(recv_message)
    if parsed_sw == 0:
        return 0, "", ""
    recv_message = recv_message[parsed_sw:]

    if len(recv_message) < 1:
        return 0, "", ""
    assert recv_message[0] == TagBuffer

    return parsed_id + parsed_sw + 1, body_client_id, body_sw_version


def decode_body_describetopicpartitions(
    recv_message: bytes,
) -> tuple[int, list[str], int, int]:
    parsed_length, array_length = decode_varint(recv_message)
    if parsed_length == 0:
        return 0, [], 0, 0
    recv_message = recv_message[parsed_length:]

    topic_array = []
    for _ in range(1, array_length):
        parsed_topic, topic = decode_string_compact(recv_message)
        if parsed_topic == 0:
            return 0, [], 0, 0
        recv_message = recv_message[parsed_topic:]

        if len(recv_message) < 1:
            return 0, [], 0, 0
        assert recv_message[0] == TagBuffer
        recv_message = recv_message[1:]

        topic_array.append(topic)
        parsed_length += parsed_topic + 1

    if len(recv_message) < 5:
        return 0, [], 0, 0
    (
        partition_limit,
        cursor,
    ) = struct.unpack(">IB", recv_message[:5])
    recv_message = recv_message[5:]

    if len(recv_message) < 1:
        return 0, [], 0, 0
    assert recv_message[0] == TagBuffer

    return parsed_length + 5 + 1, topic_array, int(partition_limit), int(cursor)


def encode_body_apiversions(api_version: int, throttle_time: int) -> bytes:
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


def encode_body_describetopicpartitions(
    topic_array: list[str], topic_dict: dict[str, bytes], throttle_time: int
) -> bytes:
    send_message = struct.pack(">I", throttle_time)
    send_message += encode_varint(1 + len(topic_array))

    for topic_name in topic_array:
        if topic_name in topic_dict:
            error_code = ErrorCode.NONE
            uuid = topic_dict[topic_name][0]
        else:
            error_code = ErrorCode.UNKNOWN_TOPIC_OR_PARTITION
            uuid = (0).to_bytes(16)
        is_internal = (0).to_bytes(1)
        auth_operations = 0x00000DF8
        send_message += struct.pack(">h", error_code)
        send_message += encode_string_compact(topic_name)
        send_message += uuid
        send_message += is_internal

        if topic_name not in topic_dict:
            send_message += encode_varint(1)

        else:
            send_message += encode_varint(1 + len(topic_dict[topic_name][1]))
            for partition in topic_dict[topic_name][1]:
                send_message += struct.pack(">h", error_code)
                send_message += struct.pack(">I", partition["partition"])
                send_message += struct.pack(">I", partition["leader"])
                send_message += struct.pack(">I", partition["leader_epoch"])

                send_message += encode_varint(1 + len(partition["replicas"]))
                for replica in partition["replicas"]:
                    send_message += struct.pack(">I", replica)

                send_message += encode_varint(1 + len(partition["insync"]))
                for insync in partition["insync"]:
                    send_message += struct.pack(">I", insync)

                send_message += encode_varint(1)
                send_message += encode_varint(1)
                send_message += encode_varint(1)

                send_message += TagBuffer.to_bytes(1)

        send_message += struct.pack("<IB", auth_operations, TagBuffer)

    next_cursor = 0xFF
    send_message += struct.pack("BB", next_cursor, TagBuffer)
    return send_message
