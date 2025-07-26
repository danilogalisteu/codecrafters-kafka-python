import logging
import struct

from .body import (
    decode_body_apiversions,
    decode_body_describetopicpartitions,
    encode_body_apiversions,
    encode_body_describetopicpartitions,
)
from .constants import ApiKey, TagBuffer
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

    if api_key == ApiKey.ApiVersions:
        parsed_body, body_client_id, body_sw_version = decode_body_apiversions(
            recv_message[4 + parsed_header :]
        )
        if parsed_body == 0:
            return 0, b""

        assert message_size == parsed_header + parsed_body, "unexpected message size"

        logging.warning(
            "Body ApiVersions: %s %s",
            body_client_id,
            body_sw_version,
        )

        throttle_time = 0
        send_message = encode_header(correlation_id)
        send_message += encode_body_apiversions(api_version, throttle_time)

        return 4 + message_size, struct.pack(">i", len(send_message)) + send_message

    if api_key == ApiKey.DescribeTopicPartitions:
        parsed_body, topic_array, partition_limit, cursor = (
            decode_body_describetopicpartitions(recv_message[4 + parsed_header :])
        )
        if parsed_body == 0:
            return 0, b""

        assert message_size == parsed_header + parsed_body, "unexpected message size"

        logging.warning(
            "Body DescribeTopicPartitions : %s %s %s",
            topic_array,
            partition_limit,
            cursor,
        )

        throttle_time = 0
        send_message = encode_header(correlation_id) + TagBuffer.to_bytes(1)
        send_message += encode_body_describetopicpartitions(topic_array, throttle_time)

        return 4 + message_size, struct.pack(">i", len(send_message)) + send_message

    return 0, b""
