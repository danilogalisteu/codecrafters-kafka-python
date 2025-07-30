import logging
import struct

from .body import (
    decode_body_apiversions,
    decode_body_describetopicpartitions,
    encode_body_apiversions,
    encode_body_describetopicpartitions,
)
from .constants import ApiKey, RecordType, TagBuffer
from .header import decode_header, encode_header


async def parse_message(
    recv_message: bytes,
    batches: dict[
        str,
        int | list[dict[str, str | int | bytes | list[int] | list[bytes] | list[str]]],
    ],
) -> tuple[int, bytes]:
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
            "Body DescribeTopicPartitions: %s %s %s",
            topic_array,
            partition_limit,
            cursor,
        )

        topic_dict = {
            record["name"]: record["topic_uuid"]
            for batch in batches
            for record in batch["records"]
            if record["record_type"] == RecordType.TOPIC
            if record["name"] in topic_array
        }

        topic_dict = {
            topic_name: (
                topic_id,
                [
                    record
                    for batch in batches
                    for record in batch["records"]
                    if record["record_type"] == RecordType.PARTITION
                    if record["topic_uuid"] == topic_id
                ],
            )
            for topic_name, topic_id in topic_dict.items()
        }

        throttle_time = 0
        send_message = encode_header(correlation_id) + TagBuffer.to_bytes(1)
        send_message += encode_body_describetopicpartitions(
            topic_array, topic_dict, throttle_time
        )

        return 4 + message_size, struct.pack(">i", len(send_message)) + send_message

    return 0, b""
